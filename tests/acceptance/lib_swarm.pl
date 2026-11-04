#!/usr/bin/perl
# Local swarm fixtures for the container acceptance carriers.
#
# The containers run only the released binary: no build step, no python.  A swarm
# that the daemon can download from is therefore synthesized here with the
# tools the runtime image already has (Perl core + coreutils):
#
#   tracker   a minimal HTTP BitTorrent tracker (compact peer list, 5s
#             interval), the same shape the Rust acceptance suite's MiniTracker
#             returns.  Every announce rewrites the state file, so a carrier can
#             wait for "the client announced" instead of sleeping.
#   peer      a protocol peer implementing exactly four messages: handshake,
#             bitfield, unchoke and PIECE replies.  It never sends an extended
#             handshake (the extension bit is advertised, then ignored) and
#             never sends `have`, so a carrier that needs those must not rely
#             on this fixture.  Modes: seed (declares every piece and serves the
#             bytes), --leecher (declares none, keeps the swarm non-empty),
#             --stall (unchokes, never answers a REQUEST — a connected but
#             non-delivering seeder), --delay-handshake SECS (the peer becomes
#             usable at a chosen point inside a read's wait), --serve-after SECS
#             (complete the handshake at once, then hold every PIECE answer
#             back until SECS after the peer started, so the read is parked
#             past a window while a seeder already counts as connected).
#
# Both run in one non-blocking select loop, so state stays in one process (the
# tracker's peer table is read by the same process that answers announces).
#
# State files are rewritten atomically and hold `key=value` lines:
#
#   tracker: announces=N / peer <info_hash> <ip>:<port> <left>
#   peer:    port=N / served=N / requests=N
#
# Usage:
#   perl lib_swarm.pl tracker --port-file F --state-file F [--port N] [--interval N]
#   perl lib_swarm.pl peer    --info-hash HEX --piece-length N --content FILE
#                             [--content FILE ...] [--port-file F] [--announce URL]
#                             [--leecher] [--stall] [--delay-handshake SECS]
#                             [--serve-after SECS] [--start-delay SECS]
#                             [--state-file F] [--peer-id 20-ASCII-BYTES]
use strict;
use warnings;
use IO::Select;
use IO::Socket::INET;
use Getopt::Long qw(GetOptions);
use POSIX qw(ceil);

my $mode = shift @ARGV // die "usage: lib_swarm.pl tracker|peer ...\n";

sub write_state {
    my ($path, $text) = @_;
    my $tmp = "$path.tmp";
    open(my $fh, '>', $tmp) or return;
    print $fh $text;
    close $fh;
    rename($tmp, $path);
}

sub url_decode {
    my ($s) = @_;
    $s =~ s/\+/ /g;
    $s =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/ge;
    return $s;
}

sub peer_id_for {
    my ($port) = @_;
    my $id = sprintf('-SP0001-%012d', $port);
    return substr($id, 0, 20);
}

if ($mode eq 'tracker') {
    my ($port_file, $state_file, $port, $interval, $access_log);
    $interval = 5;
    $port = 0;
    GetOptions(
        'port-file=s'  => \$port_file,
        'state-file=s' => \$state_file,
        'port=i'       => \$port,
        'interval=i'   => \$interval,
        'access-log=s' => \$access_log,
    ) or die "bad options\n";

    my $listen = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        LocalPort => $port,
        Proto     => 'tcp',
        Listen    => 64,
        ReuseAddr => 1,
    ) or die "tracker: bind failed: $!\n";
    my $bound = $listen->sockport;
    if ($port_file) {
        write_state($port_file, "PORT=$bound\nURL=http://127.0.0.1:$bound/announce\n");
    }

    my %peers;    # info_hash => { "$ip:$port" => left }
    my $announces = 0;
    my $select = IO::Select->new();
    $select->add($listen);
    my %pending;    # fileno => { sock => , buf => , peer_ip => }

    my $flush_state = sub {
        my $out = "announces=$announces\n";
        for my $hash (sort keys %peers) {
            my $hex = unpack('H*', $hash);
            for my $key (sort keys %{ $peers{$hash} }) {
                $out .= "peer $hex $key " . $peers{$hash}{$key} . "\n";
            }
        }
        write_state($state_file, $out) if $state_file;
    };
    $flush_state->();

    while (1) {
        my @ready = $select->can_read(0.5);
        for my $fh (@ready) {
            if ($fh == $listen) {
                my $client = $listen->accept();
                next unless $client;
                $client->blocking(0);
                my $ip = $client->peerhost // '127.0.0.1';
                $pending{ fileno($client) } = { sock => $client, buf => '', ip => $ip };
                $select->add($client);
                next;
            }
            my $key = fileno($fh);
            my $conn = $pending{$key} or next;
            my $read = sysread($fh, my $chunk, 65536);
            if (!defined $read || $read == 0) {
                $select->remove($fh);
                delete $pending{$key};
                close $fh;
                next;
            }
            $conn->{buf} .= $chunk;
            next unless $conn->{buf} =~ /\r\n\r\n/;
            $select->remove($fh);
            delete $pending{$key};

            my ($line) = split(/\r\n/, $conn->{buf});
            my ($target) = $line =~ m{^GET\s+(\S+)\s+HTTP};
            my $body;
            if (defined $target && $target =~ m{^/announce}) {
                my %q;
                my ($query) = $target =~ /\?(.*)$/;
                for my $pair (split(/&/, $query // '')) {
                    my ($k, $v) = split(/=/, $pair, 2);
                    next unless defined $k;
                    $q{$k} = url_decode($v // '');
                }
                my $hash = $q{info_hash} // '';
                my $peer_port = $q{port} // 6881;
                my $left = $q{left} // 0;
                $announces++;
                if (length($hash) == 20) {
                    $peers{$hash}{"$conn->{ip}:$peer_port"} = $left;
                }
                my $compact = '';
                if (length($hash) == 20) {
                    for my $key (sort keys %{ $peers{$hash} }) {
                        next if $key eq "$conn->{ip}:$peer_port";
                        my ($ip, $p) = split(/:/, $key);
                        $compact .= pack('C4n', split(/\./, $ip), $p);
                    }
                }
                $body = 'd8:intervali' . $interval . 'e5:peers'
                    . length($compact) . ':' . $compact . 'e';
                if ($access_log) {
                    open(my $lf, '>>', $access_log) or die "tracker: $access_log: $!\n";
                    printf $lf "%d %s from=%s:%s left=%s returned=%d\n",
                        time(), unpack('H*', $hash), $conn->{ip}, $peer_port, $left,
                        length($compact) / 6;
                    close $lf;
                }
                $flush_state->();
            } else {
                $body = '';
            }
            my $resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
                . length($body) . "\r\nConnection: close\r\n\r\n" . $body;
            syswrite($fh, $resp);
            close $fh;
        }
    }
}

if ($mode eq 'peer') {
    my ($hash_hex, $plen, $port_file, $announce, $leecher, $stall);
    my ($delay_handshake, $start_delay, $state_file, $peer_id, $port);
    my $serve_after = 0;
    my @content;
    $plen = 16384;
    $port = 0;
    $delay_handshake = 0;
    $start_delay = 0;
    GetOptions(
        'info-hash=s'      => \$hash_hex,
        'piece-length=i'   => \$plen,
        'content=s'        => \@content,
        'port=i'           => \$port,
        'port-file=s'      => \$port_file,
        'announce=s'       => \$announce,
        'leecher'          => \$leecher,
        'stall'            => \$stall,
        'delay-handshake=f' => \$delay_handshake,
        'serve-after=f'    => \$serve_after,
        'start-delay=f'    => \$start_delay,
        'state-file=s'     => \$state_file,
        'peer-id=s'        => \$peer_id,
    ) or die "bad options\n";
    die "peer: --info-hash and --content are required\n"
        unless defined $hash_hex && @content;

    my $info_hash = pack('H*', $hash_hex);
    die "peer: info hash must be 20 bytes\n" unless length($info_hash) == 20;

    my $blob = '';
    for my $path (@content) {
        open(my $fh, '<:raw', $path) or die "peer: open $path: $!\n";
        local $/;
        $blob .= <$fh>;
        close $fh;
    }
    my $total = length($blob);
    my $piece_count = int(($total + $plen - 1) / $plen);

    select(undef, undef, undef, $start_delay) if $start_delay > 0;

    my $listen = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        LocalPort => $port,
        Proto     => 'tcp',
        Listen    => 32,
        ReuseAddr => 1,
    ) or die "peer: bind failed: $!\n";
    my $bound = $listen->sockport;
    $peer_id //= peer_id_for($bound);
    $peer_id = substr($peer_id . ('-' x 20), 0, 20);

    my $bitfield = '';
    for my $i (0 .. $piece_count - 1) {
        my $byte = int($i / 8);
        $bitfield .= "\0" while length($bitfield) <= $byte;
        if (!$leecher) {
            substr($bitfield, $byte, 1) = chr(ord(substr($bitfield, $byte, 1)) | (0x80 >> ($i % 8)));
        }
    }

    my ($served, $requests) = (0, 0);
    my $flush_state = sub {
        write_state($state_file, "port=$bound\nserved=$served\nrequests=$requests\n")
            if $state_file;
    };
    $flush_state->();
    write_state($port_file, "PORT=$bound\n") if $port_file;

    # Announce once up front and then on the tracker's interval, so a peer that
    # starts late is still discovered by an announcing client.
    my $announce_once = sub {
        return unless $announce;
        my $escaped = join('', map { sprintf('%%%02x', ord($_)) } split(//, $info_hash));
        my $left = $leecher ? $total : 0;
        my ($host, $path) = $announce =~ m{^http://([^/]+)(/.*)$};
        return unless $host;
        my $path_q = $path . '?info_hash=' . $escaped . '&peer_id=' . $peer_id
            . '&port=' . $bound . '&left=' . $left . '&compact=1&event=started';
        my $sock = IO::Socket::INET->new(PeerAddr => $host, Proto => 'tcp', Timeout => 5);
        return unless $sock;
        syswrite($sock, "GET $path_q HTTP/1.1\r\nHost: $host\r\nConnection: close\r\n\r\n");
        my $sink = '';
        sysread($sock, $sink, 4096);
        close $sock;
    };
    $announce_once->();
    my $last_announce = time();

    # --serve-after: the handshake completes at once, but PIECE answers stay
    # queued until this instant, so a read is provably parked past a window
    # while the swarm already counts this peer as a connected seeder.
    my $serve_at = time() + $serve_after;
    my @pending_pieces;    # [fileno, index, begin, size]

    my $select = IO::Select->new();
    $select->add($listen);
    my %conns;    # fileno => { sock, in, out, hs_done, hs_at }

    my $frame = sub {
        my ($id, $payload) = @_;
        return pack('N', 1 + length($payload)) . chr($id) . $payload;
    };

    while (1) {
        if (@pending_pieces && time() >= $serve_at) {
            for my $p (splice @pending_pieces) {
                my ($key, $index, $begin, $size) = @$p;
                my $c = $conns{$key} or next;
                my $start = $index * $plen + $begin;
                my $end = $start + $size;
                $end = $total if $end > $total;
                next if $start >= $total || $end <= $start;
                my $payload = pack('N2', $index, $begin) . substr($blob, $start, $end - $start);
                $c->{out} .= $frame->(7, $payload);
                $served++;
            }
            $flush_state->();
        }
        if (time() - $last_announce >= 5) {
            $announce_once->();
            $last_announce = time();
        }
        my $now = time();
        for my $key (keys %conns) {
            my $c = $conns{$key};
            next if $c->{hs_done} || !defined $c->{hs_at};
            if ($now >= $c->{hs_at}) {
                my $hs = chr(19) . 'BitTorrent protocol' . "\0\0\0\0\0\x10\0\0"
                    . $info_hash . $peer_id;
                $c->{out} .= $hs . $frame->(1, '') . $frame->(5, $bitfield);
                $c->{hs_done} = 1;
            }
        }

        my $wait = 0.2;
        my @readable = $select->can_read($wait);
        for my $fh (@readable) {
            if ($fh == $listen) {
                my $client = $listen->accept();
                next unless $client;
                $client->blocking(0);
                $conns{ fileno($client) } = {
                    sock  => $client,
                    in    => '',
                    out   => '',
                    hs_done => ($delay_handshake <= 0 ? 0 : 0),
                    hs_at => ($delay_handshake > 0 ? time() + $delay_handshake : 0),
                };
                $select->add($client);
                next;
            }
            my $key = fileno($fh);
            my $c = $conns{$key} or next;
            my $read = sysread($fh, my $chunk, 65536);
            if (!defined $read || $read == 0) {
                $select->remove($fh);
                close $fh;
                delete $conns{$key};
                next;
            }
            $c->{in} .= $chunk;
            # Strip the peer's 68-byte handshake once.
            if (length($c->{in}) >= 68 && !$c->{saw_hs}) {
                substr($c->{in}, 0, 68, '');
                $c->{saw_hs} = 1;
            }
            while (length($c->{in}) >= 4) {
                my $len = unpack('N', substr($c->{in}, 0, 4));
                last if length($c->{in}) < 4 + $len;
                my $msg = substr($c->{in}, 4, $len, '');
                substr($c->{in}, 0, 4, '');
                next if $len == 0;
                my $id = ord(substr($msg, 0, 1));
                if ($id == 6 && $len >= 13) {
                    $requests++;
                    next if $stall;
                    my ($index, $begin, $size) = unpack('N3', substr($msg, 1, 12));
                    my $start = $index * $plen + $begin;
                    my $end = $start + $size;
                    $end = $total if $end > $total;
                    next if $start >= $total || $end <= $start;
                    if ($serve_after > 0 && time() < $serve_at) {
                        push @pending_pieces, [ $key, $index, $begin, $size ];
                        $flush_state->();
                        next;
                    }
                    my $payload = pack('N2', $index, $begin) . substr($blob, $start, $end - $start);
                    $c->{out} .= $frame->(7, $payload);
                    $served++;
                    $flush_state->();
                }
            }
        }

        my @writable = $select->can_write(0.05);
        for my $fh (@writable) {
            next if $fh == $listen;
            my $key = fileno($fh);
            my $c = $conns{$key} or next;
            next unless length $c->{out};
            my $written = syswrite($fh, $c->{out});
            next unless defined $written;
            substr($c->{out}, 0, $written, '');
        }
    }
}

die "lib_swarm.pl: unknown mode '$mode'\n";
