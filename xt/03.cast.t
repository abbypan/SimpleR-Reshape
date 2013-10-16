#!/usr/bin/perl
use utf8;
use lib '../lib';
use SimpleR::Reshape;
use Test::More ;
use Data::Dump qw/dump/;

my $r = cast('02.melt.csv', 
    cast_filename => '03.cast.csv', 

    names => [ qw/day hour state key value/ ], 
    id => [ 0, 1, 2 ],
    measure => 3, 
    value => 4, 

    stat_sub => sub { my ($vlist) = @_; my @temp = sort { $b <=> $a } @$vlist; return $temp[0] }, 
    result_names => [ qw/day hour state cnt rank/ ], 
);
#dump($r);

done_testing;




