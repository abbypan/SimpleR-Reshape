# ABSTRACT: Reshape like R
package SimpleR::Reshape;

require Exporter;
@ISA    = qw(Exporter);
@EXPORT = qw(read_table write_table melt cast merge);

our $VERSION     = 0.03;
our $DEFAULT_SEP = ',';


sub read_table {
    my ( $txt, %opt ) = @_;

    $opt{sep}             //= $DEFAULT_SEP;
    $opt{skip_head}       //= 0;
    $opt{return_arrayref} //= 1;
    $opt{write_sub} = gen_sub_write_row( $opt{write_filename}, %opt )
      if ( exists $opt{write_filename} );

    my @data;
    my $row_deal_sub = sub {
        my ($row) = @_;

        return if ( exists $opt{skip_sub} and $opt{skip_sub}->($row) );
        my @s = exists $opt{conv_sub} ? $opt{conv_sub}->($row) : $row;
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


sub gen_sub_write_row {
    my ( $txt, %opt ) = @_;
    $opt{sep} ||= $DEFAULT_SEP;

    my $write_format = $opt{charset} ? ">:$opt{charset}" : ">";
    open my $fh, $write_format, $txt;

    if($opt{header}){
        print $fh join($opt{sep}, @{$opt{header}}), "\n";
    }

    my $w_sub = sub {
        my ($r) = @_;

        #支持嵌套一层ARRAY
        my @data = map {  ref($_) eq 'ARRAY' ? @$_ : $_ } @$r;

        print $fh join( $opt{sep}, @data ), "\n";
    };
    return $w_sub;
}

sub write_table {
    my ($data, %opt) = @_;
    my $w_sub = gen_sub_write_row($opt{file}, %opt);
    $w_sub->($_) for @$data;
    return $opt{file};
}

sub melt {

    my ( $data, %opt ) = @_;

    my $names = $opt{names};
    if ( !exists $opt{measure} ) {
        my %selected_id = map { $_ => 1 } @{ $opt{id} };
        my @var_index = grep { !exists $selected_id{$_} } ( 0 .. $#$names );
        $opt{measure} = \@var_index;
    }

    $opt{conv_sub} = sub {
        my ($r) = @_;
        my @s;
        my @id_cols = @{$r}[ @{ $opt{id} } ];
        push @s, [ @id_cols, $names->[$_], $r->[$_] ] for @{ $opt{measure} };
        return @s;
    };

    $opt{write_filename} = $opt{melt_filename};
    return read_table( $data, %opt );
}

sub cast_row_cell {
    my ($r, $x) = @_;
    my $v =  ref($x) eq 'CODE' ?  $x->($r) : $r->[ $x ];
    return $v;
}

sub cast {
    my ( $data, %opt ) = @_;
    $opt{stat_sub} ||= sub { $_[0][0] };

    my %kv;
    my %measure_name;
    $opt{conv_sub} = sub {
        my ($r) = @_;

        my $k = join( $opt{sep}, @{$r}[ @{ $opt{id} } ] );
        if ( !exists $kv{$k} ) {
            my %temp;
            $temp{ $opt{names}[$_] } = $r->[$_] for @{ $opt{id} };
            $kv{$k} = \%temp;
        }

        my $v_name =  cast_row_cell($r, $opt{measure});
        $measure_name{$v_name} = 1;

        my $v =  cast_row_cell($r, $opt{value});
        push @{ $kv{$k}{$v_name} }, $v;

        if(exists $opt{reduce_sub}){
            my $tmp = $opt{reduce_sub}->($kv{$k}{$v_name});
            $kv{$k}{$v_name} =  $tmp ;
        }
        return;
    };
    read_table( $data, %opt );

    read_table(
        \%kv,
        conv_sub => sub {
            my ($r) = @_;
            for my $m_name ( keys(%measure_name) ) {
                my $stat_v =
                  exists $r->{$m_name} ? 
                    $opt{stat_sub}->( $r->{$m_name} ) : 0;
                $r->{$m_name} = $stat_v;
            }
            $r->{$_} //= 0 for(@{ $opt{result_names} });
            return [ @{$r}{ @{ $opt{result_names} } } ];
        },
        write_filename => $opt{cast_filename},
    );
}

sub merge {
    my ($x, $y, %opt) = @_;

    my @raw = (
        { data => $x, by => $opt{by_x} || $opt{by}, 
            value => $opt{value_x} || $opt{value} || [ 0 .. $#{$x->[0]} ],  }, 
        { data => $y, by => $opt{by_y} || $opt{by}, 
            value => $opt{value_y} || $opt{value} || [ 0 .. $#{$y->[0]} ],  }, 
    );

    my %main;
    my @cut_list;
    for my $i (0 .. $#raw){
        my ($d, $by) = @{$raw[$i]}{qw/data by/};
        for my $row (@$d) {
            my $cut = join($opt{sep}, @{$row}[ @$by ]);
            push @cut_list, $cut unless ( exists $main{$cut} );
            $main{$cut}[$i] = $row;
        }
    }
    @cut_list = sort @cut_list;

    my @result;
    for my $cut (@cut_list) {
        my @vs  = split qr/$opt{sep}/, $cut;
        for my $i (0 .. $#raw){
            my $d = $main{$cut}[$i];
            my $vlist = $raw[$i]{value};

            push @vs, $d ? ($d->[$_] // '') : '' for (@$vlist);
        }
        push @result, \@vs;
    }

    return \@result;
}
1;
