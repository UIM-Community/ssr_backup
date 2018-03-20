use strict;
use warnings;
use lib "D:/apps/Nimsoft/perllib";
use lib "D:/apps/Nimsoft/Perl64/lib/Win32API";
use Data::Dumper;
use DBI;
use threads;
use Thread::Queue;
use threads::shared;
use Nimbus::API;
use Nimbus::CFG;
use Nimbus::PDS;
use Perluim::API;
use Perluim::Addons::CFGManager;
use Perluim::Core::Events;
use Lib::selfservice;
use Lib::ssrv2table;
use Lib::reftable;
$Data::Dumper::Deparse = 1;

# Global variables
$Perluim::API::Debug = 1;
my ($STR_Properties,$STR_Login,$STR_Password,$STR_ExportDirectory,$STR_SQLMaxDay,$STR_Import_NBThreads,$STR_NBThreads,$STR_Interval,$STR_MaxProfiles,$INT_GetGroups,$INT_GetSpecifics,$STR_TableName,$SSRCli);
my ($STR_Alarm_Subsys,$INT_Alarm_Severity);
my $INT_DoubleSync;
my $STR_RefDBName;
my $STR_NIMDomain;
my $BOOL_DEBUG;
my $BOOL_ExportLock = 0;
my $BOOL_MainRunning = 0;
my $HASH_Robot;
my $Probe_NAME  = "ssr_backup";
my $Probe_VER   = "1.0";
my $Probe_CFG   = "ssr_backup.cfg";
$SIG{__DIE__} = \&scriptDieHandler;

#
# Register logger!
# 
my $Logger = uimLogger({
    file => "ssr_backup.log",
    level => 6
});

#
# scriptDieHandler
#
sub scriptDieHandler {
    my ($err) = @_; 
    print "$err";
    $Logger->fatal($err);
    exit(1);
}

#
# Init and configuration configuration
#
sub read_configuration {
    $Logger->nolevel("---------------------------------");
    $Logger->info("Read and parse configuration file!");
    my $CFGManager = Perluim::Addons::CFGManager->new($Probe_CFG,1);

    $BOOL_DEBUG              = $CFGManager->read("setup","debug",0);
    my $INT_LogLVL           = $CFGManager->read("setup","loglevel",6);
    $Logger->setLevel($INT_LogLVL);
    $Logger->trace( $CFGManager ) if $BOOL_DEBUG;
    $STR_Properties          = $CFGManager->read("setup","properties");
    $STR_Login               = $CFGManager->read("setup","login","administrator");
    $STR_Password            = $CFGManager->read("setup","password");
    $STR_ExportDirectory     = $CFGManager->read("setup","export_directory","export");
    $STR_NBThreads           = $CFGManager->read("setup","export_max_threads",2);
    $STR_Import_NBThreads    = $CFGManager->read("setup","import_max_threads",1);
    $STR_Interval            = $CFGManager->read("setup","interval",86400);
    $STR_MaxProfiles         = $CFGManager->read("setup","max_profiles",3);
    $STR_SQLMaxDay           = $CFGManager->read("setup","sqltable_max_day",180);
    $INT_GetSpecifics        = $CFGManager->read("setup","get_specifics",1);
    $INT_GetGroups           = $CFGManager->read("setup","get_groups",1);
    $BOOL_ExportLock         = $CFGManager->read("setup","export_lock",0);
    $INT_DoubleSync          = $CFGManager->read("setup","double_sync",1);

    $STR_Alarm_Subsys        = $CFGManager->read("alarm","subsys","1.1.20");
    $INT_Alarm_Severity      = $CFGManager->read("alarm","severity",5);

    $STR_TableName           = $CFGManager->read("refDB","table","ssrbackup_ref");
    $STR_RefDBName           = $CFGManager->read("refDB","database","CMDB_Import");

    my $STR_JAVACMD          = $CFGManager->read("setup","java_cmd","java -Xmx32m -jar selfservice-cli.jar");

    # Close script if some properties are missing!
    scriptDieHandler("SSR Properties file path is not defined!") if !defined $STR_Properties;
    $SSRCli = Lib::selfservice->new($STR_JAVACMD,$STR_Properties);
    $Logger->trace( $SSRCli );
    $Logger->catch( $SSRCli );

    if($BOOL_DEBUG) {
        $Lib::selfservice::Debug = 1;
        $Lib::ssrv2table::Debug = 1;
        $SSRCli->on('debug', sub {
            my $msg = shift; 
            $Logger->debug($msg);
        });
    }
    else {
        $Lib::selfservice::Debug = 0;
        $Lib::ssrv2table::Debug = 0;
        $SSRCli->{Emitter}->remove_subscriber('debug') if $SSRCli->{Emitter}->has_subscriber('debug');
    }

    # Create directory!
    $Logger->info("Create childs directory!");
    mkdir($STR_ExportDirectory);
    mkdir("output");
    mkdir("$STR_ExportDirectory\\groups") if $INT_GetGroups;
    mkdir("$STR_ExportDirectory\\devices") if $INT_GetSpecifics;

    # Links vars
    $Lib::reftable::STR_ExportDirectory = $STR_ExportDirectory;
    $Lib::reftable::DBName = $STR_TableName;
    $Lib::reftable::Logger = $Logger;
    $Lib::reftable::RefDB = $STR_RefDBName;
    $Lib::ssrv2table::Logger = $Logger;
    $Logger->nolevel("---------------------------------");
}
read_configuration();

# Login to Nimbus!
nimLogin("$STR_Login","$STR_Password") if defined $STR_Login && defined $STR_Password;

# Find Nimsoft Domain!
$Logger->info("Get nimsoft domain...");
{
    my ($RC,$STR_Domain) = nimGetVarStr(NIMV_HUBDOMAIN);
    scriptDieHandler("Failed to get domain!") if $RC != NIME_OK;
    $STR_NIMDomain = $STR_Domain;
}

$Logger->info("DOMAIN => $STR_NIMDomain");

# Get local robot info ! 
{
    my ($request,$response);
    $request = uimRequest({
        addr => "controller",
        callback => "get_info",
        retry => 3,
        timeout => 5
    });
    $response = $request->send(1);
    scriptDieHandler("Failed to get information for local robot") if not $response->rc(NIME_OK);
    $HASH_Robot = $response->hashData();
}

# Echo information about the robot where the script is started!
$Logger->info("HUBNAME => $HASH_Robot->{hubname}");
$Logger->info("ROBOTNAME => $HASH_Robot->{robotname}");
$Logger->info("VERSION => $HASH_Robot->{version}");
$Logger->nolevel("--------------------------------");

#
# Register probe
# 
my $probe = uimProbe({
    name    => $Probe_NAME,
    version => $Probe_VER
});
$Logger->trace( $probe );

# Register callbacks (String and Int are valid type for arguments)
$Logger->info("Register probe callbacks...");
$probe->registerCallback( "get_info" );
$probe->registerCallback( "lock_export", { val => "Int" } );
$probe->registerCallback( "reset" , { deleteLocalFiles => "Int" });
$probe->registerCallback( "import_group" , { 
    groupOrigin => "String", 
    profileName => "String", 
    groupDestination => "String", 
    version => "Int" 
});
$probe->registerCallback( "import_specific" , { 
    profileName => "String", 
    deviceName => "String", 
    version => "Int" 
});

# Probe restarted
$probe->on( restart => sub {
    $Logger->info("Probe restarted");
    read_configuration();
});

# Probe timeout
$probe->on( timeout => sub {});

# Start detached thread for import!
$Logger->info("Create all import threads...");
my $import_q = Thread::Queue->new();
{ 
    my $import_threadSub = sub {
        $Logger->warn("Import Thread started!");
        while ( defined ( my $str = $import_q->dequeue() ) ) {
            my ($type,$id,$filePath) = split('##',$str);
            $Logger->info("Import profile in thread executed... id => $id, type => $type");
            $SSRCli->device_import($id,$filePath) if $type eq "device";
            $SSRCli->group_import($id,$filePath) if $type eq "group";
        }
        $Logger->warn("Import Thread finished!");
    };
    my @importThr = map {
        threads->create(\&$import_threadSub);
    } 1..$STR_Import_NBThreads;
    $_->detach for @importThr;
}

# interval thread!
$Logger->info("Create main interval checker thread...");
my $mainTimer = threads->create(sub {
    sleep 5;
    for (;;) {
        my $start = time;
        $Logger->nolevel("--------------------------------");
        $Logger->warn("Interval executed...");
        main();
        if ((my $remaining = $STR_Interval - (time - $start)) > 0) {
            sleep $remaining;
        }
    }
});
$mainTimer->detach();

# Start probe!
$probe->start();
$Logger->nolevel("--------------------------------");

#
# Main method (called in timeout callback of the probe).
#
sub main {
    my $DB;
    my (@GroupExport,@SpecificExport);

    if($BOOL_ExportLock) {
        $Logger->warn("Export stopped because export lock is on!");
        my $supp_key = "${Probe_NAME}_exportlock";
        my %AlarmObject = (
            severity => $INT_Alarm_Severity,
            message => "SSR_Backup export is locked!",
            robot => "$HASH_Robot->{robotname}",
            domain => $STR_NIMDomain,
            probe => "$Probe_NAME",
            origin => "$HASH_Robot->{origin}",
            source => "$HASH_Robot->{hubip}",
            dev_id => "$HASH_Robot->{robot_device_id}",
            subsystem => $STR_Alarm_Subsys,
            suppression => $supp_key,
            supp_key => $supp_key,
            usertag1 => "$HASH_Robot->{os_user1}",
            usertag2 => "$HASH_Robot->{os_user2}"
        );
        my ($PDS,$alarmid) = generateAlarm('alarm',\%AlarmObject);
        $Logger->warn("New alarm generate with id => $alarmid");
        my ($rc_alarm,$res) = nimRequest("$HASH_Robot->{robotname}",48001,"post_raw",$PDS->data);

        if($rc_alarm == NIME_OK) {
            $Logger->success("Alarm sent successfully!");
        }
        else {
            $Logger->error("Failed to sent new alarm!");
        }
        return;
    }

    if($BOOL_MainRunning) {
        $Logger->warn("Main is already runned...");
        return;
    }
    $BOOL_MainRunning = 1;

    # Main closer handler!
    sub closeMain {
        my ($msg,$error) = @_;
        $Logger->fatal($msg) if defined $msg;
        $Logger->error($error) if defined $error;
        goto end;
    }

    eval {
        $DB = Lib::ssrv2table::connect_database($Probe_CFG);
    };
    closeMain("Failed to establish a connection to the database",$@) if $@;

    # Delete old updated rows!
    $Logger->nolevel("--------------------------------");
    $Logger->info("Delete old SQL rows where created > $STR_SQLMaxDay Days");
    $Logger->nolevel("--------------------------------");
    Lib::reftable::delete_old($DB,$STR_SQLMaxDay);

    goto doublesyncEnd if $INT_DoubleSync == 0;

    # Rewind local to ref table!
    $Logger->nolevel("--------------------------------");
    $Logger->info("Get unsynchronised SQL rows...");
    Lib::ssrv2table::use_db("$STR_RefDBName");
    my @ProfilesToDelete = ();

    { # Sync groups...
        my $sth = $DB->prepare("SELECT profileId,profileName,template,group_id,groupName,ver_id FROM $STR_TableName WITH (NOLOCK) WHERE device_id IS NULL");
        $sth->execute;

        while(my $hashRef = $sth->fetchrow_hashref) {
            my $profileId   = $hashRef->{profileId};
            my $profileName = $hashRef->{profileName};
            my $versionId   = $hashRef->{ver_id};
            my $groupId     = $hashRef->{group_id};
            my $templateId  = $hashRef->{template};
            my $groupName   = $hashRef->{groupName}; 

            $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
            $groupName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

            my $XMLPath = "$STR_ExportDirectory\\groups\\${groupId}_${groupName}\\${groupId}_${groupName}_${templateId}_${profileName}_v${versionId}.xml";
            if(!-e $XMLPath) {
                push(@ProfilesToDelete,{
                    profileId => $profileId,
                    versionId => $versionId
                });
            }
        }
        $sth->finish;
    }

    { # Sync devices...
        my $sth = $DB->prepare("SELECT profileId,profileName,cs_id,template,deviceName,ver_id FROM $STR_TableName WITH (NOLOCK) WHERE group_id IS NULL");
        $sth->execute;

        while(my $hashRef = $sth->fetchrow_hashref) {
            my $profileId   = $hashRef->{profileId};
            my $profileName = $hashRef->{profileName};
            my $versionId   = $hashRef->{ver_id};
            my $templateId  = $hashRef->{template};
            my $deviceName  = $hashRef->{deviceName}; 
            my $cs_id       = $hashRef->{cs_id};

            $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

            my $XMLPath = "$STR_ExportDirectory\\devices\\${cs_id}_${deviceName}\\${cs_id}_${deviceName}_${templateId}_${profileId}_${profileName}_v${versionId}.xml";
            if(!-e $XMLPath) {
                push(@ProfilesToDelete,{
                    profileId => $profileId,
                    versionId => $versionId
                });
            }
        }
        $sth->finish;
    }

    eval {
        $DB->begin_work;
        foreach(@ProfilesToDelete) {
            my $sth = $DB->prepare("DELETE FROM $STR_TableName WHERE profileId = ? AND ver_id = ?");
            $sth->execute($_->{profileId},$_->{versionId});
            $Logger->info("Delete profileID => $_->{profileId} with version $_->{versionId} from ${STR_RefDBName}.${STR_TableName}");
        }
        $DB->commit;
    };
    if($@) {
        $DB->rollback;
        closeMain("Failed to synchronise local XML files with the SQL table $STR_TableName",$@);
    }
    undef @ProfilesToDelete;

    doublesyncEnd:
    goto step_two if $INT_GetGroups == 0; 

    # GET SQL Rows
    $Logger->nolevel("--------------------------------");
    $Logger->info("Get groups profiles from CA_UIM.SSRV2Profile table and compare with ref table!");
    $Logger->nolevel("--------------------------------");
    eval {
        @GroupExport = Lib::reftable::group_policy($DB,$STR_MaxProfiles); 
    };
    closeMain("Failed to get groups profiles",$@) if $@;

    my $COUNT_Group     = scalar @GroupExport;
    $Logger->nolevel("--------------------------------");
    $Logger->nolevel("Group count => $COUNT_Group");
    $Logger->nolevel("--------------------------------");

    if($COUNT_Group > 0) {
        # Enqueue for group profiles! (base on group_id)
        $Logger->info("Enqueue groups...");
        my $q_group = Thread::Queue->new();
        $q_group->enqueue("$_->{profileId}##$_->{profileName}##$_->{group_id}##$_->{template}##$_->{ver}##$_->{groupName}") for @GroupExport;
        $q_group->end();

        # Group export thread function
        my $groups_export;
        $groups_export = sub {
            $Logger->warn("Thread started!");
            while ( defined ( my $profileStr = $q_group->dequeue_nb() ) ) {
                my $nb = $q_group->pending();
                if(!defined $nb) {
                    $nb = 0;
                }
                my ($profileId,$profileName,$groupId,$templateId,$versionId,$groupName) = split('##',$profileStr);
                if(!defined $profileId || !defined $profileName || !defined $groupId || !defined $templateId || !defined $groupName) {
                    $Logger->fatal("Invalid profile!");
                    next;
                }

                $Logger->info("profileName => <$profileName>, groupName => $groupName, version => $versionId, queueNB => $nb");
                $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
                $groupName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
                eval {
                    my $directoryPath = "$STR_ExportDirectory\\groups\\${groupId}_${groupName}";
                    if( !(-d $directoryPath) ) {
                        mkdir($directoryPath) or warn "Failed to create export ${groupId}_${groupName} directory";
                    }
                };
                if($@) {
                    $Logger->error("$@");
                }

                $SSRCli->profile_export({
                    profileId => $profileId,
                    group => $groupId,
                    fileLocation => "$STR_ExportDirectory\\groups\\${groupId}_${groupName}\\${groupId}_${groupName}_${templateId}_${profileName}_v${versionId}.xml"
                });
            }
            $Logger->success("Thread finished!");
            return 1;
        };

        # Wait for group threads
        my @thr = map {
            threads->create(\&$groups_export);
        } 1..$STR_NBThreads;
        $_->join() for @thr;

    }
    else {
        $Logger->info("No groups profiles to export!");
    }

    step_two:
    goto end if $INT_GetSpecifics == 0;

    $Logger->nolevel("--------------------------------");
    $Logger->info("Get specific profiles from CA_UIM.SSRV2Profile table and compare with ref table!");
    $Logger->nolevel("--------------------------------");
    eval {
        @SpecificExport = Lib::reftable::specific_policy($DB,$STR_MaxProfiles);
    };
    closeMain("Failed to get specifics/devices profiles",$@) if $@;

    my $COUNT_Specific  = scalar @SpecificExport;
    $Logger->nolevel("--------------------------------");
    $Logger->nolevel("Specific count => $COUNT_Specific");
    $Logger->nolevel("--------------------------------");

    if($COUNT_Specific > 0) {
        # Enqueue for specific profile (base on device_id)

        $Logger->info('Enqueue devices...');
        my $q_specific = Thread::Queue->new();
        $q_specific->enqueue("$_->{profileId}##$_->{profileName}##$_->{device_id}##$_->{template}##$_->{ver}##$_->{deviceName}##$_->{cs_id}") for @SpecificExport;
        $q_specific->end();

        # Specific export thread function
        my $specific_export;
        $specific_export = sub {
            $Logger->warn("Thread started!");
            while ( defined ( my $profileStr = $q_specific->dequeue_nb() ) ) {
                my $nb = $q_specific->pending();
                if(!defined $nb) {
                    $nb = 0;
                }
                my ($profileId,$profileName,$deviceId,$templateId,$versionId,$deviceName,$cs_id) = split('##',$profileStr);
                if(!defined $profileId || !defined $profileName || !defined $deviceId || !defined $templateId || !defined $deviceName || !defined $cs_id) {
                    $Logger->fatal("Invalid profile!");
                    next;
                }

                $Logger->info("profileName => <$profileName>, deviceName => $deviceName, cs_id => $cs_id, version => $versionId, queueNB => $nb");
                $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
                eval {
                    my $directoryPath = "$STR_ExportDirectory\\devices\\${cs_id}_${deviceName}";
                    if( !(-d $directoryPath) ) {
                        mkdir($directoryPath) or warn "Failed to create ${cs_id}_${deviceName} directory";
                    }
                };
                if($@) {
                    $Logger->error("$@");
                }

                $SSRCli->profile_export({
                    profileId => $profileId,
                    device => $cs_id,
                    fileLocation => "$STR_ExportDirectory\\devices\\${cs_id}_${deviceName}\\${cs_id}_${deviceName}_${templateId}_${profileId}_${profileName}_v${versionId}.xml"
                });
            }
            $Logger->success("Thread finished!");
            return 1;
        };
    
        # Wait for specific threads
        my @thr = map {
            threads->create(\&$specific_export);
        } 1..$STR_NBThreads;
        $_->join() for @thr;

    }
    else {
        $Logger->info("No specific profiles to export!");
    }

    # GOTO END
    end:

    # Close database!
    $Logger->info("Disconnect database cursor...") if defined $DB;
    $DB->disconnect() if defined $DB;

    $BOOL_MainRunning = 0;
    # Copy log file in ouput directory!
    eval {
        my $T = getDate();
        createDirectory("output/$T");
        $Logger->copyTo("output/$T");
    };
    if($@) {
        $Logger->error("Failed to copy logfile!");
    }
}

#
# get_info callback!
#
sub get_info {
    my ($hMsg) = @_;
    $Logger->info("get_info callback triggered !");
    nimSendReply($hMsg,NIME_OK);
}

#
# lock_export callback!
#
sub lock_export {
    my ($hMsg,$val) = @_;
    $Logger->info("lock_export callback triggered !");
    $Logger->info("arg 'Val' value => $val");
    if(!defined $val || $val eq "NULL" || $val eq "") {
        $val = 1;
    }
    $BOOL_ExportLock = $val; 

    $Logger->info("New export lock value => $BOOL_ExportLock");
    nimSendReply($hMsg,NIME_OK);
}

#
# reset callback
#
sub reset {
    my ($hMsg,$deleteLocalFiles) = @_;
    if(!defined $deleteLocalFiles) {
        $deleteLocalFiles = 1;
    }

    # Connect database!
    my $DB;
    eval {
        $DB = Lib::ssrv2table::connect_database($Probe_CFG);
        Lib::ssrv2table::use_db("$STR_RefDBName");
    };
    if($@) {
        $Logger->error($@);
        nimSendReply($hMsg,NIME_ERROR);
        return;
    }

    # Truncate table!
    $Logger->info("Truncate table!");
    my $sth = $DB->prepare("TRUNCATE TABLE $STR_TableName");
    $sth->execute;
    $sth->finish;

    # Send ok!
    nimSendReply($hMsg,NIME_OK);
    $DB->disconnect;

    # Delete local files!
    if($deleteLocalFiles) {
        $Logger->info("Delete local files!");
        my $filespec = "*.xml";
        my @dirs = ($STR_ExportDirectory);

        while (@dirs) {
            my $curdir = pop @dirs;
            opendir(my $dh, $curdir);
            for my $f (readdir($dh)) {
                next if $f =~ /^\.\.?$/;
                next unless -d "$curdir/$f";
                push @dirs, "$curdir/$f";
            }

            for (glob("$curdir/$filespec")) {
                $Logger->info("Deleting XML file => $_");
                unlink $_;
            }
        }
        $Logger->success("All files successfully deleted!");
    }
}

#
# Import group callback!
#
sub import_group {
    my ($hMsg,$version,$profileName,$groupOrigin,$groupDestination) = @_; 
    $Logger->nolevel('------------------------------------');
    $Logger->info("import_group callback triggered!");

    # Check defaults arguments!
    if(!defined $version) {
        $version = 1;
    }

    if(!defined $groupOrigin || !defined $profileName || !defined $groupDestination) {
        $Logger->error("Please defined groupOrigin,profileName and groupDestination fields");
        nimSendReply($hMsg,NIME_ERROR);
        return;
    }

    # Connect and use right database!
    my $DB;
    eval {
        $DB = Lib::ssrv2table::connect_database($Probe_CFG);
        Lib::ssrv2table::use_db("$STR_RefDBName");
    };
    if($@) {
        $Logger->error($@);
        nimSendReply($hMsg,NIME_ERROR);
        return;
    }

    my $sth;
    my $offset = $version - 1;
    eval {
        if(defined($profileName) && $profileName ne "NULL") { # Import only one
            $sth = $DB->prepare("SELECT profileName,template,group_id,groupName,ver_id FROM $STR_TableName WITH(NOLOCK) WHERE profileName = ? AND groupName = ? ORDER BY updated OFFSET $offset ROWS FETCH NEXT 1 ROWS ONLY");
            $sth->execute($profileName,$groupOrigin);
            my $rows = $sth->rows; 

            if($rows == 0) {
                $Logger->warn("No entry in SQL table for profile => $profileName in group => $groupOrigin");
                nimSendReply($hMsg,NIME_NOENT);
                return;
            }

            nimSendReply($hMsg,NIME_OK);
            while(my $hashRef = $sth->fetchrow_hashref) {
                my $groupId     = $hashRef->{group_id};
                my $groupName   = $hashRef->{groupName};
                my $templateId  = $hashRef->{template};
                my $profileName = $hashRef->{profileName};
                my $versionId   = $hashRef->{ver_id};

                $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
                $groupName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

                my $filePath    = "$STR_ExportDirectory\\groups\\${groupId}_${groupName}\\${groupId}_${groupName}_${templateId}_${profileName}_v${versionId}.xml";
                $Logger->info("Import XML Path => $filePath");
                $Logger->info("Import Group => $groupDestination");
                $import_q->enqueue("group##${groupDestination}##${filePath}");
                last;
            }

            $sth->finish;
        }
        else { # Import multiple
            $sth = $DB->prepare("SELECT DISTINCT(profileName) FROM $STR_TableName WITH(NOLOCK) WHERE groupName = ?");
            $sth->execute($groupOrigin);
            my $rows = $sth->rows; 

            if($rows == 0) {
                $Logger->warn("No entry in SQL table for group => $groupOrigin");
                nimSendReply($hMsg,NIME_NOENT);
                return;
            }

            my @RawRef = ();
            while(my $hashRef = $sth->fetchrow_hashref) {
                push(@RawRef,$hashRef);
            }

            $sth->finish;
            nimSendReply($hMsg,NIME_OK);
            foreach my $hashRef (@RawRef) {
                my $child_profileName = $hashRef->{profileName};

                my $sth2 = $DB->prepare("SELECT profileName,template,group_id,groupName,ver_id FROM $STR_TableName WITH(NOLOCK) WHERE profileName = ? AND groupName = ? ORDER BY updated OFFSET $offset ROWS FETCH NEXT 1 ROWS ONLY");
                $sth2->execute($child_profileName,$groupOrigin);
                my $cRows = $sth->rows; 

                if($cRows == 0) {
                    $Logger->warn("No entry in SQL table for children profile => $child_profileName");
                    next;
                }

                cWhile: while(my $childRef = $sth2->fetchrow_hashref) {
                    my $groupId     = $childRef->{group_id};
                    my $groupName   = $childRef->{groupName};
                    my $templateId  = $childRef->{template};
                    my $profileName = $childRef->{profileName};
                    my $versionId   = $childRef->{ver_id};

                    $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
                    $groupName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

                    my $filePath    = "$STR_ExportDirectory\\groups\\${groupId}_${groupName}\\${groupId}_${groupName}_${templateId}_${profileName}_v${versionId}.xml";
                    $Logger->info("Import XML Path => $filePath");
                    $Logger->info("Import Group => $groupDestination");
                    $import_q->enqueue("group##${groupDestination}##${filePath}");

                    $sth2->finish;
                    last cWhile;
                }
            }

        }
    };
    $Logger->error("Failed to import group profile : $@") if $@;

    $DB->disconnect; # Disconnect DB
}

#
# Import specific callback!
# 
sub import_specific {
    my ($hMsg,$version,$profileName,$deviceName) = @_;
    $Logger->nolevel('------------------------------------');
    $Logger->info("import_specific callback triggered!");

    # Check defaults arguments!
    if(!defined $version) {
        $version = 1;
    }

    if(!defined $deviceName) {
        $Logger->error("Please provide at least a deviceName");
        nimSendReply($hMsg,NIME_ERROR);
        return;
    }

    # Connect and use right database!
    my $DB;
    eval {
        $DB = Lib::ssrv2table::connect_database($Probe_CFG);
        Lib::ssrv2table::use_db("$STR_RefDBName");
    };
    if($@) {
        $Logger->error($@);
        nimSendReply($hMsg,NIME_ERROR);
        return;
    }

    my $sth;
    my $offset = $version - 1;
    eval {
        if(defined($profileName) && $profileName ne "NULL") { # Import one
            $sth = $DB->prepare("SELECT profileId,profileName,template,deviceName,cs_id,ver_id FROM $STR_TableName WITH(NOLOCK) WHERE profileName = ? AND deviceName = ? ORDER BY updated OFFSET $offset ROWS FETCH NEXT 1 ROWS ONLY");
            $sth->execute($profileName,$deviceName);
            my $rows = 0; 

            my @RawRef = ();
            while(my $hashRef = $sth->fetchrow_hashref) {
                $rows++;
                push(@RawRef,$hashRef);
            }
             $sth->finish;

            if($rows == 0) {
                $Logger->warn("No entry in SQL table for profile => $profileName for device => $deviceName");
                nimSendReply($hMsg,NIME_NOENT);
                return;
            }

            nimSendReply($hMsg,NIME_OK);
            foreach my $hashRef (@RawRef) {
                my $deviceName  = $hashRef->{deviceName};
                my $templateId  = $hashRef->{template};
                my $profileId   = $hashRef->{profileId};
                my $profileName = $hashRef->{profileName};
                my $versionId   = $hashRef->{ver_id};
                my $cs_id       = $hashRef->{cs_id};

                $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

                my $filePath    = "$STR_ExportDirectory\\devices\\${cs_id}_${deviceName}\\${cs_id}_${deviceName}_${templateId}_${profileId}_${profileName}_v${versionId}.xml";
                $Logger->info("Import XML Path => $filePath");
                $Logger->info("Import Device cs_id => $cs_id");
                $import_q->enqueue("device##${cs_id}##${filePath}");
                last;
            }

        }
        else { # Import multiple
            $sth = $DB->prepare("SELECT DISTINCT(profileName) FROM $STR_TableName WITH(NOLOCK) WHERE deviceName = ?");
            $sth->execute($deviceName);
            my $rows = 0; 

            my @RawRef = ();
            while(my $hashRef = $sth->fetchrow_hashref) {
                $rows++;
                push(@RawRef,$hashRef);
            }

            if($rows == 0) {
                $Logger->warn("No entry in SQL table for device => $deviceName");
                nimSendReply($hMsg,NIME_NOENT);
                return;
            }
            $sth->finish;

            nimSendReply($hMsg,NIME_OK);
            foreach my $hashRef (@RawRef) {
                my $child_profileName = $hashRef->{profileName};
                my $sth2 = $DB->prepare("SELECT profileId,profileName,template,deviceName,cs_id,ver_id FROM $STR_TableName WITH(NOLOCK) WHERE profileName = ? AND deviceName = ? ORDER BY updated OFFSET $offset ROWS FETCH NEXT 1 ROWS ONLY");
                $sth2->execute($child_profileName,$deviceName);
                my $cRows = $sth2->rows; 

                if($cRows == 0) {
                    $Logger->warn("No entry in SQL table for children profile => $child_profileName");
                    next;
                }

                cWhile: while(my $childRef = $sth2->fetchrow_hashref) {
                    my $deviceName  = $childRef->{deviceName};
                    my $templateId  = $childRef->{template};
                    my $profileId   = $childRef->{profileId};
                    my $profileName = $childRef->{profileName};
                    my $versionId   = $childRef->{ver_id};
                    my $cs_id       = $childRef->{cs_id};

                    $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;

                    my $filePath    = "$STR_ExportDirectory\\devices\\${cs_id}_${deviceName}\\${cs_id}_${deviceName}_${templateId}_${profileId}_${profileName}_v${versionId}.xml";
                    $Logger->info("Import XML Path => $filePath");
                    $Logger->info("Import Device cs_id => $cs_id");
                    $import_q->enqueue("device##${cs_id}##${filePath}");
                    $sth2->finish;
                    last cWhile;
                }
            }
        }
    };
    $Logger->error("Failed to import device profile : $@") if $@;

    $DB->disconnect; # Disconnect DB
}
