package Lib::sqldate;
use strict;
use warnings;
use 5.010;
use Time::Piece;

sub new {
    my ($class,$dateString) = @_;
    my $this = {
        originalString => $dateString,
        year => undef,
        month => undef,
        day => undef,
        hour => undef,
        min => undef,
        sec => undef,
        finalDate => undef
    };
    my $blessed = bless($this,ref($class) || $class);
    $blessed->parse();
    return $blessed;
}

sub parse {
    my ($self) = @_;
    $self->{year}  = substr($self->{originalString},0,4);
    $self->{month} = substr($self->{originalString},5,2);
    $self->{day}   = substr($self->{originalString},8,2);
    $self->{hour}  = substr($self->{originalString},11,2);
    $self->{min}   = substr($self->{originalString},14,2);
    $self->{sec}   = substr($self->{originalString},17,2);

    my $date = sprintf("%02d:%02d:%02d %02d:%02d:%02d",$self->{year},$self->{month},$self->{day},$self->{hour},$self->{min},$self->{sec});
    my $format = '%Y:%m:%d %H:%M:%S';
    $self->{finalDate} = Time::Piece->strptime($date, $format);
}

sub getDate {
    my ($self) = @_;
    return $self->{finalDate};
}

sub compare {
    my ($self,$sqlDate) = @_;
    return $sqlDate->getDate() - $self->getDate();
}

1;
