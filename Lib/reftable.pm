package Lib::reftable;
use Lib::ssrv2table;
use Lib::sqldate;

our @EXPORT_OK = qw(group_policy specific_policy delete_old);
our $STR_ExportDirectory;
our $DBName;
our $Logger; 
our $RefDB;

sub delete_old {
    my ($DB,$DayT) = @_;

    # Use new database!
    eval {
        Lib::ssrv2table::use_db("$RefDB");
    };
    if($@) {
        $Logger->error($@);
        return;
    }

    # Select all old rows!
    my $sth = $DB->prepare("SELECT * FROM $DBName WITH(NOLOCK) WHERE created < DATEADD(day, -$DayT, GETDATE())");
    $sth->execute;

    my @RawRef  = (); 
    my $rows    = 0;
    while(my $hashRef = $sth->fetchrow_hashref) {
        $rows++;
        push(@RawRef,$hashRef);
    }
    $Logger->info("Row to delete from $DBName => $rows");
    $sth->finish;

    goto end if $rows == 0;
    eval {
        $DB->begin_work;
        foreach my $hashRef(@RawRef) {
            my $profileId   = $hashRef->{profileId};
            my $profileName = $hashRef->{profileName};
            my $versionId   = $hashRef->{ver_id};

            my $delete = $DB->prepare("DELETE FROM $DBName WHERE profileId = ? and ver_id = ?");
            $delete->execute($profileId,$versionId);
            if(defined($hashRef->{device_id})) {
                _deleteLocalFile({
                    profileId   => $profileId,
                    profileName => $profileName,
                    template    => $hashRef->{template},
                    device_id   => $hashRef->{device_id},
                    deviceName  => $hashRef->{deviceName}
                },"devices",$versionId);
            }
            else {
                _deleteLocalFile({
                    profileName => $profileName,
                    template    => $hashRef->{template},
                    group_id    => $hashRef->{group_id},
                    groupName   => $hashRef->{groupName}
                },"groups",$versionId);
            }
            $delete->finish;
        }
        $DB->commit;
    };
    if($@) {
        $Logger->error("Failed to delete old rows: $@");
        $DB->rollback;
    }

    # GOTO END
    end:
}

sub _deleteLocalFile {
    my ($profileHash,$type,$version) = @_;
    my ($std,$filePath); 
    my $profileName = $profileHash->{profileName};
    $profileName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
    if($type eq "groups") {
        my $groupName = $profileHash->{groupName};
        $groupName =~ s/[\:\\\/\<\>\*\?\"\|]//g;
        $std = "$profileHash->{group_id}_${groupName}";
    }
    else {
        $std = "$profileHash->{device_id}_$profileHash->{deviceName}";
    }
    if($type eq "groups") {
        $filePath = "$STR_ExportDirectory\\$type\\$std\\$std_$profileHash->{template}_${profileName}_v${version}.xml";
    }
    else {
        $filePath = "$STR_ExportDirectory\\$type\\$std\\$std_$profileHash->{template}_$profileHash->{profileId}_${profileName}_v${version}.xml";
    }
    $Logger->info("Delete XMLFile: $filePath");
    unlink($filePath) if -e $filePath;
}

sub _profileExist {
    my ($DB,$profileID) = @_;
    my $sth = $DB->prepare("SELECT profileId,updated,ver_id FROM $DBName WITH (NOLOCK) WHERE profileId = ? ORDER BY created DESC");
    $sth->execute($profileID);
    my $updated;
    my $ver_id;
    my $rows = 0;
    while(my $hashRef = $sth->fetchrow_hashref) {
        if($rows == 0) {
            $updated = $hashRef->{updated};
            $ver_id  = $hashRef->{ver_id};
        }
        $rows++;
    }
    $sth->finish;
    return $rows,$updated,$ver_id;
}

sub _deleteProfile {
    my ($DB,$profileId) = @_;
    my $sth = $DB->prepare("WITH q AS (SELECT TOP 1 * FROM ssrbackup_ref WITH(NOLOCK) WHERE profileId = ? ORDER BY updated) DELETE FROM q");
    $sth->execute($profileId);
    $sth->finish;
}

sub _createGroupProfile {
    my ($DB,$profileHash,$version) = @_;
    my $ver = defined $version ? $version : 1;
    my $sth = $DB->prepare("INSERT INTO $DBName (profileId,profileName,template,group_id,updated,ver_id,groupName) VALUES (?,?,?,?,?,?,?)");
    $sth->execute(
        $profileHash->{profileId},
        $profileHash->{profileName},
        $profileHash->{template},
        $profileHash->{group_id},
        $profileHash->{updated},
        $ver,
        $profileHash->{groupName}
    );
    $sth->finish;
}

sub _createSpecificProfile {
    my ($DB,$profileHash,$version) = @_;
    my $ver = defined $version ? $version : 1;
    my $sth = $DB->prepare("INSERT INTO $DBName (profileId,profileName,template,device_id,updated,ver_id,cs_id,deviceName) VALUES (?,?,?,?,?,?,?,?)");
    $sth->execute(
        $profileHash->{profileId},
        $profileHash->{profileName},
        $profileHash->{template},
        $profileHash->{device_id},
        $profileHash->{updated},
        $ver,
        $profileHash->{cs_id},
        $profileHash->{deviceName}
    );
    $sth->finish;
}

sub group_policy {
    my ($DB,$MaxProfiles) = @_; 

    # Get profiles from CA_UIM.SSRV2Profiles
    eval {
        Lib::ssrv2table::use_db("CA_UIM");
    };
    warn "Failed to use CA_UIM datase..." if $@; 

    my @GroupExport = Lib::ssrv2table::group_policy(); 

    eval {
        Lib::ssrv2table::use_db("$RefDB");
    };
    warn "Failed to use $RefDB datase..." if $@; 
    $Logger->nolevel("--------------------------------");

    my @profiles = ();
    my @updated_profiles = ();
    my @deleted_profiles = (); 

    foreach my $dbHash (@GroupExport) {
        my ($rowCount,$updateStr,$ver_id) = _profileExist($DB,$dbHash->{profileId});
        if($rowCount == 0) {
            $dbHash->{ver} = 1;
            push(@profiles,$dbHash);
            next;
        }

        my $ref_dt      = Lib::sqldate->new($updateStr);
        my $ssrv2_dt    = Lib::sqldate->new($dbHash->{updated});

        my $diff        = $ref_dt->compare($ssrv2_dt);
        next if $diff == 0;
        $Logger->info("$dbHash->{profileId}/$dbHash->{profileName} :: Difference => $diff, versionId => $ver_id, $updateStr <> $dbHash->{updated}");

        # Delete one profile if to much are stored!
        $dbHash->{ver}  = $ver_id + 1;
        my $ProfileNB   = $rowCount + 1;
        if($ProfileNB > $MaxProfiles) {
            $Logger->info("Delete one row from ssrbackup_ref table! (And delete local file if exist!)");
            push(@deleted_profiles,$dbHash->{profileId});
            _deleteLocalFile($dbHash,"groups",$dbHash->{ver} - $MaxProfiles);
        }
        push(@updated_profiles,$dbHash);
    }

    # Delete profile!
    eval {
        $DB->begin_work; 
        _deleteProfile($DB,$_) for @deleted_profiles;
        $DB->commit;
    };
    if($@) {
        $DB->rollback;
        $Logger->error("Failed to delete groups profiles in bulk: $@");
    }

    # Create profile!
    eval {
        $DB->begin_work;
        _createGroupProfile($DB,$_) for @profiles;
        _createGroupProfile($DB,$_,$_->{ver}) for @updated_profiles;
        $DB->commit;
    };
    if($@) {
        $DB->rollback;
        $Logger->error("Failed to create groups profiles in bulk: $@");
    }

    push(@profiles,$_) for @updated_profiles;
    return @profiles;
}

sub specific_policy {
    my ($DB,$MaxProfiles) = @_; 

    # Get profiles from CA_UIM.SSRV2Profiles
    eval {
        Lib::ssrv2table::use_db("CA_UIM");
    };
    warn "Failed to use CA_UIM datase..." if $@; 

    my @SpecificExport = Lib::ssrv2table::specific_policy(); 

    eval {
        Lib::ssrv2table::use_db("$RefDB");
    };
    warn "Failed to use $RefDB datase..." if $@; 
    $Logger->nolevel("--------------------------------");

    my @profiles = ();
    my @updated_profiles = ();
    my @deleted_profiles = (); 

    foreach my $dbHash (@SpecificExport) {
        my ($rowCount,$updateStr,$ver_id) = _profileExist($DB,$dbHash->{profileId});
        if($rowCount == 0) {
            $dbHash->{ver} = 1;
            push(@profiles,$dbHash);
            next;
        }
        my $ref_dt      = Lib::sqldate->new($updateStr);
        my $ssrv2_dt    = Lib::sqldate->new($dbHash->{updated});

        my $diff        = $ref_dt->compare($ssrv2_dt);
        next if $diff == 0;
        $Logger->info("$dbHash->{profileId}/$dbHash->{profileName} :: Difference => $diff, versionId => $ver_id, $updateStr <> $dbHash->{updated}");

        # Delete one profile if to much are stored!
        $dbHash->{ver}  = $ver_id + 1;
        my $ProfileNB   = $rowCount + 1;
        if($ProfileNB > $MaxProfiles) {
            $Logger->info("Delete one row from ssrbackup_ref table! (And delete local file if exist!)");
            push(@deleted_profiles,$dbHash->{profileId});
            _deleteLocalFile($dbHash,"devices",$dbHash->{ver} - $MaxProfiles);
        }
        push(@updated_profiles,$dbHash);
    }

    # Delete profile!
    eval {
        $DB->begin_work; 
        _deleteProfile($DB,$_) for @deleted_profiles;
        $DB->commit;
    };
    if($@) {
        $DB->rollback;
        $Logger->error("Failed to delete specific profiles in bulk: $@");
    }

    # Create profiles!
    eval {
        $DB->begin_work;
        _createSpecificProfile($DB,$_) for @profiles;
        _createGroupProfile($DB,$_,$_->{ver}) for @updated_profiles;
        $DB->commit;
    };
    if($@) {
        $DB->rollback;
        $Logger->error("Failed to create specific profiles in bulk: $@");
    }

    push(@profiles,$_) for @updated_profiles;
    return @profiles;
}

1;