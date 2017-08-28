#!/usr/bin/perl

use lib '../lib';
use SimpleR::Reshape;
use Data::Dumper;

my $df = [
  [ 'a', '1' ],
  [ 'b', '2' ],
  [ 'c', '2' ],
];

my $new_df = map_cast_col(
  $df,
  cast_col    => 1,
  map_col_sub => sub {
    my ( $r, $s ) = @_;
    return $r->[1] / $s;
  },
);

print Dumper( $new_df );

#$VAR1 = [
#[ 'a', 1, '0.2' ],
#[ 'b', 2, '0.4' ],
#[ 'c', 2, '0.4' ]
#];
