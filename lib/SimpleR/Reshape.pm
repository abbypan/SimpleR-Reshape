# ABSTRACT: Reshape data like R
package SimpleR::Reshape;

require Exporter;
@ISA    = qw(Exporter);
@EXPORT = qw(read_table write_table melt cast merge split_file);

our $VERSION     = 0.06;
our $DEFAULT_SEP = ',';

sub read_table {
    my ( $txt, %opt ) = @_;

    $opt{sep} //= $DEFAULT_SEP;
    $opt{skip_head} //= 0;
    $opt{write_sub} = write_row_sub( $opt{write_file}, %opt )
      if ( exists $opt{write_file} );
    $opt{return_arrayref} //= exists $opt{write_file} ? 0 : 1;

    my @data;
    if ( $opt{write_head} ) {
        $opt{write_sub}->( $opt{write_head} ) if ( exists $opt{write_sub} );
        push @data, $opt{write_head} if ( $opt{return_arrayref} );
    }

    my $row_deal_sub = sub {
        my ($row) = @_;

        return if ( $opt{skip_sub} and $opt{skip_sub}->($row) );
        my @s = $opt{conv_sub} ? $opt{conv_sub}->($row) : $row;
        return unless (@s);

        if ( exists $opt{write_sub} ) {
            $opt{write_sub}->($_) for @s;
        }

        push @data, @s if ( $opt{return_arrayref} );
    };

    if ( -f $txt ) {
        my $read_format = $opt{charset} ? "<:$opt{charset}" : "<";
        open my $fh, $read_format, $txt;
        <$fh> if ( $opt{skip_head} );
        while ( my $d = <$fh> ) {
            chomp($d);
            my @temp = split $opt{sep}, $d;
            $row_deal_sub->( \@temp );
        }
    }
    elsif ( ref($txt) eq 'ARRAY' ) {
        my $i = $opt{skip_head} ? 1 : 0;
        $row_deal_sub->( $txt->[$_] ) for ( $i .. $#$txt );
    }
    elsif ( ref($txt) eq 'HASH' ) {
        while ( my ( $tk, $tr ) = each %$txt ) {
            $row_deal_sub->($tr);
        }
    }

    return \@data;
}

sub write_row_sub {
    my ( $txt, %opt ) = @_;
    $opt{sep} ||= $DEFAULT_SEP;

    my $write_format = $opt{charset} ? ">:$opt{charset}" : ">";
    open my $fh, $write_format, $txt;

    if ( $opt{head} ) {
        print $fh join( $opt{sep}, @{ $opt{head} } ), "\n";
    }

    my $w_sub = sub {
        my ($r) = @_;

        #支持嵌套一层ARRAY
        my @data = map { ref($_) eq 'ARRAY' ? @$_ : $_ } @$r;

        print $fh join( $opt{sep}, @data ), "\n";
    };
    return $w_sub;
}

sub write_table {
    my ( $data, %opt ) = @_;
    my $w_sub = write_row_sub( $opt{file}, %opt );
    $w_sub->($_) for @$data;
    return $opt{file};
}

sub melt {
    my ( $data, %opt ) = @_;

    my $names = $opt{names};
    if ( !exists $opt{measure} and ref( $opt{id} ) eq 'ARRAY' ) {
        my %s_id = map { $_ => 1 } map_arrayref_value( $opt{id} );
        $opt{measure} = [ grep { !exists $s_id{$_} } ( 0 .. $#$names ) ];
    }

    $opt{conv_sub} = sub {
        my ($r) = @_;
        my @id_cols = map_arrayref_value( $opt{id}, $r );
        my @s = map { [ @id_cols, $names->[$_], $r->[$_] ] } @{ $opt{measure} };
        return @s;
    };

    $opt{write_file} = $opt{melt_file};
    return read_table( $data, %opt );
}

sub map_arrayref_value {
    my ( $id, $arr ) = @_;

    my $t = ref($id);
    my @res =
        ( $t eq 'CODE' ) ? $id->($arr)
      : ( $t eq 'ARRAY' and $arr ) ? @{$arr}[@$id]
      : ( $t eq 'ARRAY' ) ? @$id
      : ( !$t and $id =~ /^\d+$/ ) ? $arr->[$id]
      :                             $id;

    wantarray ? @res : $res[0];
}

sub cast {
    my ( $data, %opt ) = @_;
    $opt{stat_sub} ||= sub { $_[0][0] };
    $opt{default_cell_value} //= 0;

    my %kv;
    my %measure_name;
    $opt{conv_sub} = sub {
        my ($r) = @_;

        my @vr = map_arrayref_value( $opt{id}, $r );
        my $k = join( $opt{sep}, @vr );
        if ( !exists $kv{$k} ) {
            my @kr = map_arrayref_value( $opt{id}, $opt{names} );
            my %temp = map { $kr[$_] => $vr[$_] } ( 0 .. $#kr );
            $kv{$k} = \%temp;
        }

        my $v_name = map_arrayref_value( $opt{measure}, $r );
        $measure_name{$v_name} = 1;

        my $v = map_arrayref_value( $opt{value}, $r );
        push @{ $kv{$k}{$v_name} }, $v;

        if ( exists $opt{reduce_sub} ) {
            $kv{$k}{$v_name} = $opt{reduce_sub}->( $kv{$k}{$v_name} );
        }
        return;
    };

    read_table(
        $data, %opt,
        return_arrayref => 0,
        write_head      => 0,
    );

    my @measure_name = sort keys(%measure_name);
    $opt{result_names} ||= [ @{ $opt{names} }[ @{ $opt{id} } ], @measure_name ];

    while ( my ( $k, $r ) = each %kv ) {
        for my $m_name (@measure_name) {
            $r->{$m_name} =
              exists $r->{$m_name}
              ? $opt{stat_sub}->( $r->{$m_name} )
              : $opt{default_cell_value};
        }
        $r->{$_} //= $opt{default_cell_value} for ( @{ $opt{result_names} } );
    }

    read_table(
        \%kv,
        conv_sub => sub {
            my ($r) = @_;
            my $v = [ @{$r}{ @{ $opt{result_names} } } ];
            $r = undef;
            return $v;
        },
        write_file      => $opt{cast_file},
        return_arrayref => $opt{return_arrayref},
        write_head      => $opt{write_head} ? $opt{result_names} : 0,
    );
}

sub merge {
    my ( $x, $y, %opt ) = @_;

    my @raw = (
        {
            data  => $x,
            by    => $opt{by_x} || $opt{by},
            value => $opt{value_x} || $opt{value} || [ 0 .. $#{ $x->[0] } ],
        },
        {
            data  => $y,
            by    => $opt{by_y} || $opt{by},
            value => $opt{value_y} || $opt{value} || [ 0 .. $#{ $y->[0] } ],
        },
    );

    my %main;
    my @cut_list;
    for my $i ( 0 .. $#raw ) {
        my ( $d, $by ) = @{ $raw[$i] }{qw/data by/};
        for my $row (@$d) {
            my $cut = join( $opt{sep}, @{$row}[@$by] );
            push @cut_list, $cut unless ( exists $main{$cut} );
            $main{$cut}[$i] = $row;
        }
    }
    @cut_list = sort @cut_list;

    my @result;
    for my $cut (@cut_list) {
        my @vs = split qr/$opt{sep}/, $cut;
        for my $i ( 0 .. $#raw ) {
            my $d     = $main{$cut}[$i];
            my $vlist = $raw[$i]{value};

            push @vs, $d ? ( $d->[$_] // '' ) : '' for (@$vlist);
        }
        push @result, \@vs;
    }

    return \@result;
}

sub split_file {
    my ( $f, %opt ) = @_;
    $opt{split_file} ||= $f;
    $opt{return_arrayref} //= 0;
    $opt{sep} //= $DEFAULT_SEP;

    return split_file_line( $f, %opt ) if ( exists $opt{line_cnt} );

    my %exist_fh;

    $opt{conv_sub} = sub {
        my ($r) = @_;
        return unless ($r);

        my $k = join( $opt{sep}, map_arrayref_value( $opt{id}, $r ) );
        $k =~ s#[\\\/,]#-#g;

        if ( !exists $exist_fh{$k} ) {
            my $file = "$opt{split_file}.$k";
            open $exist_fh{$k}, '>', $file;
        }

        my $fhw = $exist_fh{$k};
        print $fhw join( $opt{sep}, @$r ), "\n";

        return;
    };

    read_table( $f, %opt );
}

sub split_file_line {
    my ( $file, %opt ) = @_;
    $opt{split_file} ||= $file;

    open my $fh, '<', $file;
    my $i      = 0;
    my $file_i = 1;
    my $fhw;
    while (<$fh>) {
        if ( $i == 0 ) {
            open $fhw, '>', "$opt{split_file}.$file_i";
        }
        print $fhw $_;
        $i++;
        if ( $i == $opt{line_cnt} ) {
            $i = 0;
            $file_i++;
        }
    }
    close $fh;
}

1;
