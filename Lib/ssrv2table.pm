package Lib::ssrv2table;
use Perluim::Addons::CFGManager;

our @EXPORT_OK = qw(connect_database group_policy specific_policy use_db);
our $Debug = 0;
our $Logger;
our $DB;

sub connect_database {
    my ($Probe_CFG) = @_; 

    my $CFGManager = Perluim::Addons::CFGManager->new($Probe_CFG,1);
    $Logger->trace( $CFGManager ) if $Debug;

    my $DB_User         = $CFGManager->read("CMDB","sql_user");
    my $DB_Password     = $CFGManager->read("CMDB","sql_password");
    my $DB_SQLServer    = $CFGManager->read("CMDB","sql_host");
    my $DB_Database     = $CFGManager->read("CMDB","sql_database");
    $Logger->info("Establish connection to the database:: $DB_SQLServer");

    my $DBCursor = DBI->connect("$DB_SQLServer;UID=$DB_User;PWD=$DB_Password",{
        RaiseError => 0,
        AutoCommit => 1,
        PrintError => 0
    }) or warn "Failed to open a connection to the database!";

    $Logger->success("Successfully connected to the database!");
    $DB = $DBCursor;
    eval {
        use_db($DB_Database);
    };
    $Logger->error($@) if $@;

    return $DBCursor;
}

sub use_db {
    my ($dbName) = @_;
    my $DBQuery = "USE $dbName";
    $Logger->info("Switch database: $DBQuery");
    $DB->do( $DBQuery ) or warn "Failed to use new database!";
}

sub group_policy {
    my $sth = $DB->prepare("SELECT profileId,profileName,template,group_id,updated,G.name as groupName FROM SSRV2Profile AS SP WITH (NOLOCK) JOIN SSRV2DeviceGroup AS DG WITH (NOLOCK) ON DG.id = SP.group_id JOIN CM_GROUP AS G WITH (NOLOCK) ON G.grp_id = DG.cm_group_id WHERE group_id IS NOT NULL AND cs_id IS NULL");
    $sth->execute;
    my @R = ();
    while(my $hashRef = $sth->fetchrow_hashref) {
        push(@R,$hashRef);
    }
    $sth->finish;
    return @R;
}

sub specific_policy {
    my $sth = $DB->prepare("SELECT SP.profileId,SP.profileName,SP.template,SP.cs_id,SD.device_id,SP.updated,CS.name as deviceName FROM SSRV2Profile AS SP WITH (NOLOCK) JOIN SSRV2Device AS SD WITH (NOLOCK) ON SP.cs_id = SD.cs_id JOIN CM_COMPUTER_SYSTEM AS CS WITH (NOLOCK) ON CS.cs_id = SD.cs_id WHERE SP.ancestorprofile IS NULL AND SP.cs_id IS NOT NULL");
    $sth->execute;
    my @R = ();
    while(my $hashRef = $sth->fetchrow_hashref) {
        push(@R,$hashRef);
    }
    $sth->finish;
    return @R;
}

1;