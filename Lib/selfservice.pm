package Lib::selfservice;
use Perluim::Core::Events;
use Scalar::Util qw(looks_like_number);

our $Debug = 0;

sub new {
    my ($class,$jarPath,$propertiesFile) = @_;
    my $this = {
        jar => $jarPath,
        properties => $propertiesFile,
        Emitter => Perluim::Core::Events->new
    };
    return bless($this,ref($class) || $class);
}

sub emit {
    my ($self,$eventName,$data) = @_;
    $self->{Emitter}->emit($eventName,$data);
}

sub on {
    my ($self,$eventName,$callbackRef) = @_;
    $self->{Emitter}->on($eventName,$callbackRef);
}

sub profile_export {
    my ($self,$hashRef) = @_; 
    my $arg = "";

    if(defined $hashRef->{group}) {
        $arg.= looks_like_number($hashRef->{group}) ? "-group $hashRef->{group} " : "-group $hashRef->{group} | findstr $hashRef->{group} ";
    }
    if(defined $hashRef->{device}) {
        $arg.="-device $hashRef->{device} ";
    }

    if($arg eq "") {
        $self->emit('log','Empty argument, please provide group or device!');
        return;
    }

    if(!defined $hashRef->{profileId} || !defined $hashRef->{fileLocation}) {
        $self->emit('log','Please provid profileId & fileLocation arguments!');
        return;
    }

    eval {
        my @export_cmd = `$self->{jar} profile-export -property_file $self->{properties} $arg -profile $hashRef->{profileId} -file "$hashRef->{fileLocation}"`;
        foreach my $stdout (@export_cmd) {
            $stdout =~ s/[\n\r]//g;
            $self->emit('debug',$stdout);
        }
    };
    if($@) {
        $self->emit('error',$@);
        return 0;
    }
    return 1;
}

sub group_import {
    my ($self,$group_name,$xml_path) = @_; 
    my $debugStr = "";
    if($Debug) {
        $debugStr = '-debug';
    }
    eval {
        my @export_cmd = `$self->{jar} profile-import -property_file $self->{properties} -group $group_name $debugStr -file "$xml_path"`;
        foreach my $stdout (@export_cmd) {
            $stdout =~ s/[\n\r]//g;
            $self->emit('debug',$stdout);
        }
    };
    if($@) {
        $self->emit('error',$@);
        return 0;
    }
    return 1;
}

sub device_import {
    my ($self,$device_name,$xml_path) = @_; 
    my $debugStr = "";
    if($Debug) {
        $debugStr = '-debug';
    }
    eval {
        my @export_cmd = `$self->{jar} profile-import -property_file $self->{properties} -device $device_name $debugStr -force -file "$xml_path"`;
        foreach my $stdout (@export_cmd) {
            $stdout =~ s/[\n\r]//g;
            $self->emit('debug',$stdout);
        }
    };
    if($@) {
        $self->emit('error',$@);
        return 0;
    }
    return 1;
}

sub profile_list {
    my ($self,$hashRef) = @_; 
    my $arg = "";

    if(defined $hashRef->{group}) {
        $arg.="-group $hashRef->{group} | findstr $hashRef->{group} ";
    }
    if(defined $hashRef->{device}) {
        $arg.="-device $hashRef->{device}";
    }

    if($arg eq "") {
        $self->emit('log','Empty argument, please provide group or device!');
        return;
    }

    my @command_output = `$self->{jar} profile-list -property_file $self->{properties} $arg`;
    my @Profiles = ();
    foreach my $output_line (@command_output){
        next if chomp($output_line) eq "";
        my ($profile_id,$group_name,$template_id,$profile_name) = split /\s\s+/,$output_line;
        $profile_name =~ s/[\:\\\/\<\>\*\?\"\|]//g;
        push(@Profiles,{
            profile_id => $profile_id,
            profile_name => $profile_name,
            template_id => $template_id,
            group_name => $group_name
        });
    }
    return @Profiles;
}

1;