# ABSTRACT: Reshape data like R
package SimpleR::Reshape;

require Exporter;
@ISA    = qw(Exporter);
@EXPORT = qw(read_table write_table melt cast merge split_file);

our $VERSION     = 0.05;
our $DEFAULT_SEP = ',';

sub get_id_array_value {
    my ($id, $arr) = @_;
    ref($id) ne 'ARRAY' ?  $id :
    $arr ?  @{$arr}[ @$id ] : @$id;
}


sub split_file {
    my ($f, %opt) = @_;
    $opt{split_filename} ||= $f;
    $opt{return_arrayref} //= 0;
    $opt{sep} //= $DEFAULT_SEP;

    return split_file_line($f, %opt) if(exists $opt{line_cnt});

    my %exist_fh;

    $opt{conv_sub} = sub {
        my ($r) = @_;
        return unless($r);

        my $k = join($opt{sep}, get_id_array_value($opt{id}, $r));
        $k=~s#[\\\/,]#-#g;

        if(! exists $exist_fh{$k}){
            my $file = "$opt{split_filename}.$k";
            open $exist_fh{$k}, '>', $file;
        }

        my $fhw = $exist_fh{$k};
        print $fhw join($opt{sep}, @$r), "\n";

        return;
    };

    read_table($f, %opt);
}

sub split_file_line {
    my ($file, %opt) = @_;
    $opt{split_filename} ||= $file;

    open my $fh,'<', $file;
    my $i=0;
    my $file_i = 1;
    my $fhw;
    while(<$fh>){
        if($i==0){
            open $fhw,'>', "$opt{split_filename}.$file_i";
        }
        print $fhw $_;
        $i++;
        if($i==$opt{line_cnt}){
            $i=0;
            $file_i++;
        }
    }
    close $fh;
}

sub read_table {
    my ( $txt, %opt ) = @_;

    $opt{sep}             //= $DEFAULT_SEP;
    $opt{skip_head}       //= 0;
    $opt{write_sub} = gen_sub_write_row( $opt{write_filename}, %opt )
      if ( exists $opt{write_filename} );
    $opt{return_arrayref} //= exists $opt{write_filename} ? 0 : 1;


    my @data;

    if($opt{write_head}){
        $opt{write_sub}->($opt{write_head}) if ( exists $opt{write_sub} ) ;
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
        my %selected_id = map { $_ => 1 } get_id_array_value($opt{id});
        my @var_index = grep { !exists $selected_id{$_} } ( 0 .. $#$names );
        $opt{measure} = \@var_index;
    }

    $opt{conv_sub} = sub {
        my ($r) = @_;
        my @s;
        my @id_cols = get_id_array_value($opt{id}, $r);
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

        my @vr = get_id_array_value($opt{id}, $r);
        my $k = join( $opt{sep},  @vr );
        if ( !exists $kv{$k} ) {
            my @kr = get_id_array_value($opt{id}, $opt{names});
            my %temp = map {
                $kr[$_] => $vr[$_]
            } (0 .. $#kr);
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
    read_table( $data, %opt, 
        return_arrayref => 0, 
        write_head => 0, 
    );

    my @measure_name = sort keys(%measure_name);
    $opt{result_names} ||= [ @{$opt{names}}[@{$opt{id}}], @measure_name ];

    while(my ($k, $r) = each %kv){
        for my $m_name (@measure_name){
            my $stat_v = exists $r->{$m_name} ?  $opt{stat_sub}->( $r->{$m_name} ) : 0;
            $r->{$m_name} = $stat_v;
        }
        $r->{$_} //= 0 for(@{ $opt{result_names} });
    }

    read_table(
        \%kv,
        conv_sub => sub {
            my ($r) = @_;
            my $v = [ @{$r}{ @{ $opt{result_names} } } ];
            $r = undef;
            return  $v;
        },
        write_filename => $opt{cast_filename},
        return_arrayref => $opt{return_arrayref}, 
        write_head => $opt{write_head} ? $opt{result_names} : 0, 
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

=pod

=encoding utf8

=head1 名称

L<SimpleR::Reshape> 数据处理转换

=head1 说明

接口山寨自R语言的read.table/write.table/merge

还有reshape2包：http://cran.r-project.org/package=reshape2

=head1 函数

=begin html

实例参考<a href="xt/">xt子文件夹</a>

=end html

=head2 read_table 

支持 从文件或arrayref 按行读入数据，转换后输出新的 文件或arrayref

    my $r = read_table(
        'reshape_src.csv', 
        skip_head=>1, 
        conv_sub => sub { [ "$_[0][0] $_[0][1]", $_[0][2], $_[0][3] ] }, 

        write_filename => '01.read_table.csv', 
        #skip_sub => sub { $_[0][3]<200 }, 
        #return_arrayref => 1, 
        #write_head => [ "head_a", "key" , "value" ], 
        #sep=>',', 
        #charset=>'utf8', 
    );

=head2 write_table

将指定数据写入文本文件

    my $d = [ [qw/a b 1/], [qw/c d 2/] ]; 
    write_table($d, 
        file=> 'write_table.csv', 
        header => [ 'ka', 'kb', 'cnt'], 
        #sep => ',', 
        #charset => 'utf8', 
    );

=head2 melt

数据调整，参考R语言的reshape2包

    my $r = melt('reshape_src.csv',
        skip_head => 1, 

        names => [ qw/day hour state cnt rank/ ], 

        #skip_sub => sub { $_[0][3]<1000 }, 
        id => [ 0, 1, 2 ],
        measure => [3, 4], 
        melt_filename => '02.melt.csv',
        #return_arrayref => 0, 
    );


=head2 cast

数据重组，参考R语言的reshape2包

    my $r = cast('02.melt.csv', 
        cast_filename => '03.cast.csv', 
        #return_arrayref => 0, 
        #write_head => 0, 

        #key 有 cnt / rank 两种
        names => [ qw/day hour state key value/ ], 
        id => [ 0, 1, 2 ],
        measure => 3, 
        value => 4, 

        stat_sub => sub { my ($vlist) = @_; my @temp = sort { $b <=> $a } @$vlist; return $temp[0] }, 

        result_names => [ qw/day hour state cnt rank/ ], 

        #reduce_sub => sub { 
        #   my ($r) = @_;
        #   my $s=0 ; $s+= $_ for @$r; 
        #   return [ $s ];
        #   }, 
    );

注意：

reduce_sub 是在读取数据的过程中处理value，默认是直接push到value列表里

stat_sub 是在数据读取完毕后，对value列表进行最终统计处理

=head2 merge

合并两个dataframe，在perl中是二层数组

    my $r = merge( 
        [ [qw/a b 1/], [qw/c d 2/] ], 
        [ [qw/a b 3/], [qw/c d 4/] ], 
        by => [ 0, 1], 
        value => [2], 
    );
    # $r = [["a", "b", 1, 3], ["c", "d", 2, 4]]

=head2 split_file

把一个大文件按指定id或行数拆分成多个小文件
    
    my $src_file = '06.split_file.log';

    split_file($src_file, id => [ 0 ] ,
        # sep => ',', 
        # split_filename => '06.test.log', 
    );

    split_file($src_file, line_cnt => 400);

=cut
