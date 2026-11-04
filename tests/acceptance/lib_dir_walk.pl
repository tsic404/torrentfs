#!/usr/bin/perl
# Directory-walk fixtures for the F2/F3 acceptance cases.
#
# The design's cost criteria (docs/03-acceptance.md §J AC-91, TC-negative-05/06)
# are about the *per-request* cost of expanding and resolving a directory, so
# the measured walk has to mirror the kernel: one entry consumed per
# `getdents64`, resuming from the offset the previous entry carried.  Perl's
# `readdir` batches a whole buffer per syscall, which would hide a per-request
# scan behind the batch; this helper therefore issues the raw syscall with a
# one-entry buffer.
#
#   list DIR            print one entry name per getdents64 call
#   stat DIR NAMES      print "name size" for each name read from NAMES
#
# The buffer is sized from the entry names this repo's torrents produce
# (`NNNNN.bin`, 9 bytes → 32-byte records); a short buffer is grown until the
# kernel accepts it, reported on stderr as `BUF=n`.
use strict;
use warnings;
use Fcntl qw(O_RDONLY O_DIRECTORY);

my $mode = shift @ARGV // die "usage: lib_dir_walk.pl list DIR | stat DIR NAMES\n";

my $arch = `uname -m`;
chomp $arch;
my %getdents = (
    'x86_64'  => 217,
    'aarch64' => 61,
);
my $nr = $getdents{$arch} // die "lib_dir_walk.pl: unsupported architecture $arch\n";

sub parse_dirents {
    my ($buf, $len) = @_;
    my @entries;
    my $pos = 0;
    while ($pos + 19 <= $len) {
        my ($ino, $off, $reclen, $dtype) = unpack('Q<Q<S<C', substr($buf, $pos, 19));
        last if $reclen <= 0 || $pos + $reclen > $len;
        my $name = substr($buf, $pos + 19, $reclen - 19);
        $name =~ s/\0.*//s;
        push @entries, [ $name, $off, $reclen ];
        $pos += $reclen;
    }
    return @entries;
}

if ($mode eq 'list') {
    my $dir = shift @ARGV // die "list needs a directory\n";
    sysopen(my $fh, $dir, O_RDONLY | O_DIRECTORY) or die "open $dir: $!\n";
    my $fd = fileno($fh);

    # The smallest buffer the kernel accepts: a short buffer returns EINVAL
    # (one record does not fit), so the first accepted size is exactly one
    # record and the syscall yields one entry per call.
    my $size = 32;
    my $buf = "\0" x 4096;
    while ($size <= 4096) {
        my $n = syscall($nr, $fd, $buf, $size);
        if ($n >= 0) {
            printf STDERR "BUF=%d\n", $size;
            last;
        }
        $size += 8;
    }
    die "lib_dir_walk.pl: no workable getdents64 buffer\n" if $size > 4096;
    sysseek($fh, 0, 0) or die "rewind $dir: $!\n";

    my $total = 0;
    my $offset = 0;
    $buf = "\0" x $size;
    while (1) {
        my $n = syscall($nr, $fd, $buf, $size);
        if ($n < 0) {
            die "getdents64 failed at offset $offset: $!\n";
        }
        last if $n == 0;
        my @entries = parse_dirents($buf, $n);
        last unless @entries;
        for my $e (@entries) {
            next if $e->[0] eq '.' || $e->[0] eq '..';
            print $e->[0], "\n";
            $total++;
        }
        $offset = $entries[-1][1];
    }
    close $fh;
    printf STDERR "ENTRIES=%d\n", $total;
}

if ($mode eq 'stat') {
    my ($dir, $names) = @ARGV;
    die "stat needs DIR and NAMES\n" unless defined $names;
    open(my $nf, '<', $names) or die "open $names: $!\n";
    while (my $name = <$nf>) {
        chomp $name;
        next if $name eq '';
        my $path = "$dir/$name";
        my @st = lstat($path);
        die "lstat $path: $!\n" unless @st;
        print "$name ", $st[7], "\n";
    }
    close $nf;
}
