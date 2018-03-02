#!/usr/bin/perl  
# Here is an example code piece to select data from Oracle  
  
use strict;  
use DBI;  
  
my $host = "10.10.34.48";  
my $sid = "videodb";  
  
my $dbh = DBI->connect("dbi:Oracle:videodb", "dwetl", "dwetl") or die "Cannot connect db:$DBI::errstr\n";  
print "I have connected to the Oracle 11g R2 database!\n";  
my $sql = qq{select * from ETL_Job_Process};

my $sth = $dbh->prepare($sql);  
$sth->execute();  
  
my ($pid, $pname); #declare columns  
my @row;
#$sth->bind_columns(undef, \$pid, \$pname);  
print "The results are:\n\n";  
while ( @row = $sth->fetchrow_array() ) { #fetch rows from DataBase  
        #print "ID:$pid, --- NAME:$pname\n";  
         print "@row\n"
}


$sth->finish(); 
