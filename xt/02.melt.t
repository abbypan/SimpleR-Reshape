#!/usr/bin/perl
use utf8;
use lib '../lib';
use SimpleR::Reshape;
use Test::More ;
use Data::Dump qw/dump/;

my $r = melt('reshape_src.csv',
    skip_head => 1, 

    names => [ qw/day hour state cnt rank/ ], 

    #skip_sub => sub { $_[0][3]<1000 }, 
    id => [ 0, 1, 2 ],
    measure => [3, 4], 
    melt_filename => '02.melt.csv',
);
#dump($r);

done_testing;

