#!/usr/bin/perl
# Deterministic .torrent builder for the container acceptance carriers.
#
# The containers run the released binary only (no build step, no python), so the
# fixtures are produced from coreutils + this Perl script.  It emits the
# torrent file, its info hash and geometry, and — for the synthesized shapes —
# the payload files the torrent declares, so a carrier can compare a read
# against an authoritative copy.
#
# Shapes:
#   single   one file; its bytes come from --content
#   multi    several files (path under the torrent name) taken from --payload-dir
#   bulk     N one-byte files in one directory (directory-scale cases)
#
# The output is shell `eval`-able key=value lines.
#
# Usage:
#   perl lib_torrent_fixture.pl make --out X.torrent --name NAME [options]
#     --content FILE            single: file whose bytes the torrent declares
#     --payload-dir DIR         multi: directory holding the declared files
#     --path REL                multi: file path inside the torrent (repeatable)
#     --bulk N                  bulk: number of files
#     --bulk-prefix P           bulk: path prefix, default "bulk"
#     --bulk-size N             bulk: bytes per file, default 1
#     --fill C                  bulk: fill byte, default "a"
#     --payload-out DIR         bulk: write the payload files under DIR
#     --piece-length N          default 16384
#     --tracker URL             announce URL (repeatable: tier 0)
#     --tier URL[,URL...]       announce URL tier (repeatable: tier 1, 2, ...)
#     --private                 set the private flag
use strict;
use warnings;
use File::Path qw(make_path);
use File::Temp qw(tempfile);
use Getopt::Long qw(GetOptions);
use POSIX qw(ceil);

sub ben_str {
    my ($s) = @_;
    return length($s) . ':' . $s;
}

sub ben_int {
    my ($n) = @_;
    return 'i' . int($n) . 'e';
}

sub ben_dict {
    my (%kv) = @_;
    my $out = 'd';
    for my $k (sort keys %kv) {
        $out .= ben_str($k) . $kv{$k};
    }
    return $out . 'e';
}

sub ben_list {
    my (@items) = @_;
    return 'l' . join('', @items) . 'e';
}

sub sha1_hex {
    my ($bytes) = @_;
    my ($fh, $path) = tempfile('tfsfixXXXXXX', DIR => $ENV{TMPDIR} || '/tmp', UNLINK => 0);
    binmode $fh;
    print $fh $bytes;
    close $fh;
    my $hex = `sha1sum < '$path'`;
    unlink $path;
    die "sha1sum failed\n" unless defined $hex && $hex =~ /^([0-9a-f]{40})/;
    return $1;
}

sub sha1_hex_file {
    my ($path) = @_;
    my $hex = `sha1sum < '$path'`;
    die "sha1sum failed for $path\n" unless defined $hex && $hex =~ /^([0-9a-f]{40})/;
    return $1;
}

sub read_file {
    my ($path) = @_;
    open(my $fh, '<:raw', $path) or die "open $path: $!\n";
    local $/;
    my $bytes = <$fh>;
    close $fh;
    return $bytes;
}

my ($out, $name, $content, $payload_dir, $piece_length, $private);
my ($bulk, $bulk_prefix, $bulk_size, $bulk_fill, $payload_out);
my (@paths, @trackers, @tiers);
$piece_length = 16384;
$bulk_prefix = 'bulk';
$bulk_size = 1;
$bulk_fill = 'a';
GetOptions(
    'out=s'          => \$out,
    'name=s'         => \$name,
    'content=s'      => \$content,
    'payload-dir=s'  => \$payload_dir,
    'path=s'         => \@paths,
    'bulk=i'         => \$bulk,
    'bulk-prefix=s'  => \$bulk_prefix,
    'bulk-size=i'    => \$bulk_size,
    'fill=s'         => \$bulk_fill,
    'payload-out=s'  => \$payload_out,
    'piece-length=i' => \$piece_length,
    'tracker=s'      => \@trackers,
    'tier=s'         => \@tiers,
    'private'        => \$private,
) or die "bad options\n";
die "--out and --name are required\n" unless defined $out && defined $name;

my @files;    # [relative path, absolute path] in torrent order
my $multi = defined $bulk || (@paths && defined $payload_dir);
if (defined $content) {
    push @files, [ $name, $content ];
} elsif (defined $bulk) {
    die "--bulk needs --payload-out\n" unless defined $payload_out;
    make_path($payload_out);
    for my $i (0 .. $bulk - 1) {
        my $rel = sprintf('%s/%05d.bin', $bulk_prefix, $i);
        my $abs = "$payload_out/$rel";
        make_path(dirname($abs));
        open(my $fh, '>:raw', $abs) or die "open $abs: $!\n";
        print $fh (($bulk_fill x 1) x $bulk_size);
        close $fh;
        push @files, [ $rel, $abs ];
    }
} elsif (defined $payload_dir && @paths) {
    for my $rel (@paths) {
        push @files, [ $rel, "$payload_dir/$rel" ];
    }
} else {
    die "one of --content / --bulk / --payload-dir+--path is required\n";
}

sub dirname {
    my ($p) = @_;
    return '.' unless $p =~ m{^(.*)/[^/]+$};
    return $1;
}

# Concatenate the declared content in torrent order and hash each piece.
my $blob = '';
my $total = 0;
my @file_entries;
for my $f (@files) {
    my $bytes = read_file($f->[1]);
    $blob .= $bytes;
    $total += length($bytes);
    if ($multi) {
        push @file_entries, ben_dict(
            'length' => ben_int(length($bytes)),
            'path'   => ben_list(map { ben_str($_) } split(m{/}, $f->[0])),
        );
    }
}

my $pieces = '';
my $offset = 0;
while ($offset < $total) {
    my $chunk = substr($blob, $offset, $piece_length);
    $pieces .= pack('H*', sha1_hex($chunk));
    $offset += $piece_length;
}

my %info;
if ($multi) {
    $info{'files'}        = ben_list(@file_entries);
    $info{'name'}         = ben_str($name);
} else {
    $info{'length'} = ben_int($total);
    $info{'name'}   = ben_str($name);
}
$info{'piece length'} = ben_int($piece_length);
$info{'pieces'}       = ben_str($pieces);
$info{'private'}      = ben_int(1) if $private;

my $info_bytes = ben_dict(%info);
my $info_hash = sha1_hex($info_bytes);

my %top;
$top{'info'} = $info_bytes;
if (@trackers || @tiers) {
    my @all = @trackers;
    push @all, split(/,/, $_) for @tiers;
    if (@all == 1 && !@tiers) {
        $top{'announce'} = ben_str($all[0]);
    } else {
        $top{'announce'} = ben_str($all[0]) if @all;
        my @lists = map { ben_list(ben_str($_)) } @trackers;
        push @lists, ben_list(map { ben_str($_) } split(/,/, $_)) for @tiers;
        $top{'announce-list'} = ben_list(@lists);
    }
}

open(my $tf, '>:raw', $out) or die "open $out: $!\n";
print $tf ben_dict(%top);
close $tf;

printf "TORRENT='%s'\n", $out;
printf "INFOHASH='%s'\n", $info_hash;
printf "PIECE_LENGTH='%d'\n", $piece_length;
printf "TOTAL_SIZE='%d'\n", $total;
printf "PIECE_COUNT='%d'\n", length($pieces) / 20;
