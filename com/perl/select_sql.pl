#!/usr/bin/perl
use strict;
use DBI;

package ETL;

$ETL::ETL_DSN = $ENV{"AUTO_DSN"};
if ( !defined($ETL::ETL_DSN) ) {
   $ETL::ETL_DSN = "videodb";
}
$ETL::DWETL_DBUSER = $ENV{"AUTO_DWETL_DBUSER"};
if ( !defined($ETL::ETL_DSN) ) {
   $ETL::ETL_DSN = "dwetl";
}
$ETL::DWETL_DBPWD = $ENV{"AUTO_DWETL_DBPWD"};
if ( !defined($ETL::ETL_DSN) ) {
   $ETL::ETL_DSN = "dwetl";
}

#从环境变量获取系统所在的根路径



#路径分隔符标识
my $DIRDELI= "/";

my $sqlFile =  $ARGV[0];

sub connectDW
{
   my $tnsname=${ETL::ETL_DSN};
	 my $user=${ETL::DWETL_DBUSER};
	 my $passwd=${ETL::DWETL_DBPWD};
   my $dbh = DBI->connect("dbi:Oracle:$tnsname",$user,$passwd,
                          { AutoCommit => 1, PrintError => 0, RaiseError => 0 } ) ;

   unless ( defined($dbh) ) { return undef; }

   return $dbh;
}

main();

sub RunSP
{
	my $sth;
	my $ret;
	my $execcode;
	my $execmsg;
	my $starttime;
	my $endtime;
	my $dbh=connectDW();
	my $FALSE;
	my @row ;
    #my $sqltext1 = qq{ "select * from DWPMART.FACT_SOHU56_YNPGC_REPORT  where DATA_DATE=20171112" };
	open (SQL, "$sqlFile");
    my $sqlStatement = <SQL> ;
    $sth = $dbh->prepare($sqlStatement) or return $FALSE;
    $ret = $sth->execute();
	#print "$sqltext1\n";
#	print "first is--$ARGV[0],second is--$ARGV[1],third is--$ARGV[2]\n";
	while (@row = $sth->fetchrow_array()) {
        print "@row\n"
    }
	print "$execcode\n";
	print "$execmsg\n";

#判断执行是否成功
	if ( $execcode eq 8100 ) {
             exit 1;
           }
}

sub main
{
	RunSP()	;
}

