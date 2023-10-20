DOC

  Author   :  Theo Stienissen
  Date     :  2023
  Purpose  :  Data from Garmin watch
  Status   :  Test!!
  Contact  :  theo.stienissen@gmail.com
  @C:\Users\Theo\OneDrive\Theo\Project\Maths\RSA\rsa.sql

Garmin data export: https://www.garmin.com/en-GB/account/datamanagement/exportdata/

There are 3 parts to this solution:
1. Downloaded CSV from the activities page: activities.csv This data is loaded in the GARMIN table.
   Column names are in Dutch, because the setting on my watch are in Dutch.
2. Downloaded CSV from the activity itself. This data is loaded in the GARMIN_DETAILS table.
3. The .fit files which can be copied from the watch. Directory:  This PC\fenix 7\Internal Storage\GARMIN\Activity

Dependencies:
1. util package
2. Pipelined function get_file_name in the SYS schema
3. external job scheduler service is running

#

create or replace type text_row as object (training0 varchar2 (50),
  training1 varchar2 (50) , training2 varchar2 (50) , training3 varchar2 (50) , training4 varchar2 (50) , training5 varchar2 (50) ,
  training6 varchar2 (50) , training7 varchar2 (50) , training8 varchar2 (50) , training9 varchar2 (50) , training10 varchar2 (50),
  training11 varchar2 (50), training12 varchar2 (50), training13 varchar2 (50), training14 varchar2 (50), training15 varchar2 (50),
  training16 varchar2 (50), training17 varchar2 (50), training18 varchar2 (50), training19 varchar2 (50), training20 varchar2 (50));
  
create or replace type text_tab as table of text_row;

create or replace type int_row as object ( exercise_date date,
  training1 number (10, 2) , training2 number (10, 2) , training3 number (10, 2) , training4 number (10, 2) , training5 number (10, 2),
  training6 number (10, 2) , training7 number (10, 2) , training8 number (10, 2) , training9 number (10, 2) , training10 number (10, 2),
  training11 number (10, 2), training12 number (10, 2), training13 number (10, 2), training14 number (10, 2), training15 number (10, 2),
  training16 number (10, 2), training17 number (10, 2), training18 number (10, 2), training19 number (10, 2), training20 number (10, 2));
/ 
  
create or replace type int_tab as table of int_row;
/


-- drop table gmn_users;

-- The avatar or nick_name needs to be unique
create table gmn_users
( id            integer generated always as identity
, first_name    varchar2 (20)
, last_name     varchar2 (50)
, windows_account   varchar2 (20)
, nick_name     varchar2 (20) not null
, hr_low        number (3)    not null default 100
, hr_medium     number (3)    not null default 120
, hr_high       number (3)    not null default 140
, longitude     number
, lattitude     number);

insert into gmn_users (first_name, last_name, nick_name) values ('Theo'   , 'Stienissen', 'Theo');
insert into gmn_users (first_name, last_name, nick_name) values ('Dolly'  , 'Stienissen', 'Dolly');
insert into gmn_users (first_name, last_name, nick_name) values ('Celeste', 'Stienissen', 'Celeste');

alter table gmn_users add constraint gmn_users_pk primary key (id);
create unique index gmn_users_uk1 on gmn_users (nick_name);

create table gmn_fit_routines
( id      integer generated always as identity
, routine varchar2 (50));

insert into gmn_fit_routines (routine) values ('FitToCSV-session.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-record.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-lap.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-data.bat');

create table gmn_fit_files
( id        number generated always as identity
, user_id   integer
, file_name varchar2 (50)
, training  number (1)
, lap       number (1)
, track     number (1));

alter table gmn_fit_files add constraint gmn_fit_files_pk primary key (user_id, file_name);
alter table gmn_fit_files add constraint gmn_fit_files_fk1 foreign key (user_id) references gmn_users (id) on delete set null;

create table gmn_csv_by_column_name
( file_id      number
, field        varchar2 (50)
, val          varchar2 (60));

create table gmn_csv_by_field_name
( id           number generated always as identity
, file_id      number
, field        varchar2 (50)
, val          varchar2 (60)
, unit         varchar2 (50));

create directory garmin               as 'C:\Work\garmin';
create directory garmin_backup        as 'C:\Work\garmin\backup';

create or replace directory fit_dolly as 'C:\Work\garmin\fit_dolly';
create or replace directory fit_theo  as 'C:\Work\garmin\fit_theo';

create table gmn_session_info (
fit_id                           integer,
avg_heart_rate                   number (3),
max_heart_rate                   number (3),
enhanced_avg_speed               number (5, 2),
enhanced_max_speed               number (5, 2),
sport                            varchar2 (50),
sport_profile_name               varchar2 (50), 
start_time                       date,
start_position_lat               number,
start_position_long              number,
end_position_lat                 number,
end_position_long                number,
total_training_effect            number (5, 2),
total_anaerobic_training_effect  number (5, 2),
total_ascent                     number (6, 2),
total_descent                    number (6, 2),
total_distance                   number (7, 2),
total_calories                   number (4),
total_elapsed_time               number (9, 3),
total_fractional_ascent          number (5, 2),
total_fractional_descent         number (5, 2),
total_timer_time                 number (7, 2),
training_load_peak               number (9, 3),
avg_power                        number (5, 2),
avg_step_length                  number (8, 2),
avg_vertical_oscillation         number (4, 2),
avg_vertical_ratio               number (4, 2),
max_fractional_cadence           number (4, 2),
max_power                        number (4),
max_running_cadence              number (4),
normalized_power                 number (4),
total_strides                    number (5),
total_work                      number (7, 0));

alter table gmn_session_info add constraint gmn_session_info_pk  primary key (fit_id);

-- 5. Create a table with all the attribute fields that we are interested in
create table gmn_fit_data
( fit_id               number (6)
, id                   number (10)
, person_id            number (4)
, avg_heart_rate       number (3)
, avg_power            number (3)
, max_power            number (3)
, step_length          number (5, 1)
, avg_step_length      number (5, 1)
, distance             number (9, 2)
, total_distance       number (9, 2)
, enhanced_avg_speed   number (6, 3)
, enhanced_speed       number (6, 3)
, heart_rate           number (3)
, max_heart_rate       number (3)
, position_lat         number
, position_long        number
, power                number (3)
, start_position_lat   number
, start_position_long  number
, start_time           date
, timestamp            date
, total_training_effect number (2, 1)
, total_work           number (7)
, enhanced_respiration_rate 	number(4,2)
, threshold_heart_rate  number(3,0)
, enhanced_altitude	    number(6,2)    
, total_timer_time	    number(8,3)
, end_position_lat	    number
, end_position_long	    number
, total_calories	    number(5,0)  
, total_elapsed_time     number(8,3) 
, enhanced_max_speed     number(5,3)
, name	                varchar2 (20)
, total_ascent     	    number(5,1)  
, total_descent	     	number(5,1) 
, enhanced_max_altitude  number(6,2)             
, num_laps	            number(3,0))
 partition by list (fit_id) automatic
(partition p_1 values (1));

alter table gmn_fit_data add constraint gmn_fit_data_pk  primary key (fit_id, id, person_id);
alter table gmn_fit_data add constraint gmn_fit_data_fk1 foreign key (person_id) references gmn_users (id);


set serveroutput on size unlimited

create or replace package garmin_pkg
is
g_max_int   constant integer := power (2, 31);

procedure set_nls;

function ds_to_varchar2 (p_ds in interval day to second) return varchar2;

function to_nr (p_string in varchar2, p_col in varchar2 default null) return number;

function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second;

function to_dt (p_string in varchar2, p_col in varchar2 default null) return date;

function interval_ds_to_seconds (p_interval in interval day to second) return integer;

function seconds_to_ds_interval (p_seconds in integer) return interval day to second;

function date_offset_to_date (p_offset in integer) return date;

function semicircles_to_lon_lat (p_semicircle in integer) return number;

procedure convert_fit_file_to_csv (p_file_name in varchar2, p_routine in varchar2);

procedure parse_csv_by_column_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN');

procedure parse_csv_by_field_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN', p_skip1 boolean default true);

procedure load_session_info;

procedure load_session_details;

procedure sync_fit_files;

procedure remove_csv_files;

function get_heartrate (p_person_id in integer, p_range in integer) return integer;

function fit_data_loaded (p_fit_id in integer) return integer;

procedure reload_fit_data (p_fit_id in integer);

function analyze_data (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 40) return text_tab pipelined;

function analyze_graph (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 20) return int_tab pipelined;

end garmin_pkg;
/


create or replace package body garmin_pkg
is

procedure set_nls
is
begin
execute immediate 'alter session set NLS_DATE_FORMAT = ''YYYY/MM/DD HH24:MI:SS''';
execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';

exception when others then
  util.show_error ('Error in procedure set_nls', sqlerrm);
end set_nls;

/******************************************************************************************************************************************************************/

--
-- Convert "interval day to second" to a standard format string
--
function ds_to_varchar2 (p_ds in interval day to second) return varchar2
is 
begin 
  return lpad (extract (day from p_ds ) , 2, '0') || ' ' || lpad (extract (hour from p_ds), 2, '0') || ':' || lpad (extract (minute from p_ds), 2, '0')  || ':' || lpad (extract (second from p_ds), 2, '0');

exception when others then
  util.show_error ('Error in function ds_to_varchar2', sqlerrm);
  return null;
end ds_to_varchar2;

/******************************************************************************************************************************************************************/

--
-- Convert the different number (or tempo) formats
--
function to_nr (p_string in varchar2, p_col in varchar2 default null) return number
is 
l_nr    number (20,4);
begin
  if p_string is null or p_string = '--' then return null;
  elsif p_string like '%:%' -- tempo 
  then  l_nr := round (3600 / (60 * substr (p_string, 1, instr (p_string, ':') - 1) + substr (p_string, instr (p_string, ':') + 1)), 2);  
  else  l_nr := to_number (replace (p_string, ',', '.'));  
  end if;
  return l_nr;

exception when zero_divide then return null;
when others then
  util.show_error ('Error in procedure to_nr. Not a number: ' || p_col || '  ' || p_string , sqlerrm);
  return null;
end to_nr;

/******************************************************************************************************************************************************************/

--
-- To_dsinterval for different formats
--
function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second
is
l_ds interval day to second;
begin
  if     p_string is null or p_string = '--' then l_ds := null;
  elsif instr (p_string, ':') = 0            then l_ds :=  to_dsinterval ( '00 00:00:' || replace (p_string, ',', '.'));
  elsif instr (p_string, ':', 1, 2) = 0      then l_ds :=  to_dsinterval ( '00 00:'    || replace (p_string, ',', '.'));
  else  l_ds :=   to_dsinterval ( '00 '    || replace (p_string, ',', '.'));
  end if;
  return l_ds;
  
exception when others then
  util.show_error ('Not an interval: ' || p_col || '  ' || p_string, sqlerrm);
  return null;
end to_ds;

/******************************************************************************************************************************************************************/

--
-- Different date formats are used by the watch
--
function to_dt (p_string in varchar2, p_col in varchar2 default null) return date
is
l_date  date;
begin
set_nls;
begin
  if   instr (p_string, ':', 1, 2) > 0
  then l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI:SS');
  else l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI');
  end if;
  return l_date;
  exception when others
  then
  if   instr (p_string, ':', 1, 2) > 0
  then l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI:SS');
  else l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI');
  end if;
  return l_date;    
  end;

exception when others then
  util.show_error ('Error in function to_dt. Not a date or unknown format: ' || p_string, sqlerrm);
  return null;
end to_dt;

/******************************************************************************************************************************************************************/

--
-- Convert interval DS to seconds
--
function interval_ds_to_seconds (p_interval in interval day to second) return integer
is 
begin 
  return 86400 * extract (day from p_interval) + 3600 * extract (hour from p_interval) + 60 * extract (minute from p_interval) + extract (second from p_interval);

exception when others then 
   util.show_error ('Error in function interval_ds_to_seconds', sqlerrm);
   return null;
end interval_ds_to_seconds;

/******************************************************************************************************************************************************************/

--
-- Convert seconds to interval day to second
--
function seconds_to_ds_interval (p_seconds in integer) return interval day to second
is 
l_tot_seconds integer (6) := floor (p_seconds);
l_hours     integer (2);
l_minutes   integer (2);
l_seconds   integer (2);
l_fraction  integer (2);
l_interval  varchar2 (20);
begin 
  l_hours    := floor ( l_tot_seconds / 3600);
  l_minutes  := floor ((l_tot_seconds - l_hours * 3600) / 60);
  l_seconds  := mod (l_tot_seconds, 60); 
  l_fraction := round ((p_seconds - l_tot_seconds) * 100, 2);
  l_interval := '00 ' || lpad (to_char (l_hours), 2, '0')  || ':' || lpad (to_char (l_minutes), 2, '0')  || ':' || lpad (to_char (l_seconds), 2, '0') || '.' || lpad (to_char (l_fraction), 2, '0');
  return to_dsinterval (l_interval);

exception when others then 
   util.show_error ('Error in function seconds_to_ds_interval for: ' || p_seconds || ' : ' || l_interval, sqlerrm);
   return null;
end seconds_to_ds_interval;

/******************************************************************************************************************************************************************/

--
-- Number of seconds that have past since the 1-st of Jan 1990. Corrextion for dst?
--
function date_offset_to_date (p_offset in integer) return date
is
begin
  set_nls;
  return to_date ('31-12-1989', 'DD-MM-YYYY') + p_offset / 3600 / 24;

exception when others then 
   util.show_error ('Error in function date_offset_to_date for: ' || p_offset, sqlerrm);
   return null;
end date_offset_to_date;

/******************************************************************************************************************************************************************/

--
-- Convert simicircles to degrees longitude or lattitude
--
function semicircles_to_lon_lat (p_semicircle in integer) return number
is
begin 
  return p_semicircle * (180 / g_max_int);

exception when others then 
   util.show_error ('Error in function semicircles_to_lon_lat for: ' || p_semicircle, sqlerrm);
   return null;
end semicircles_to_lon_lat;

/******************************************************************************************************************************************************************/

-- 
-- The Oracle scheduler job is able to run external routines. Requirement is that the external job scheduler service is running
-- https://developer.garmin.com/fit/download/
-- https://developer.garmin.com/fit/protocol/
--
procedure convert_fit_file_to_csv (p_file_name in varchar2, p_routine in varchar2)
is
l_job_name    varchar2 (100) := dbms_scheduler.generate_job_name;
l_action      varchar2 (200) := 'C:\Work\garmin_bu\FitSDKRelease_21.115.00\FitSDKRelease_21.115.00\java' || chr(92) || p_routine;
begin 
   dbms_scheduler.create_job (job_name    => l_job_name,
                              job_type    => 'executable',
                              job_action  => l_action,
                              number_of_arguments => 1,
                              auto_drop   => true);
   dbms_scheduler.set_job_argument_value (l_job_name, 1, p_file_name);
   dbms_scheduler.run_job (l_job_name);
   dbms_scheduler.drop_job (l_job_name);
   commit;

exception when others then
   dbms_scheduler.drop_job (l_job_name);
   commit;
   util.show_error ('Error in procedure convert_fit_file_to_csv for: ' || p_file_name, sqlerrm);
end convert_fit_file_to_csv;

/******************************************************************************************************************************************************************/

--
-- Parse_csv_by_column_name
--
procedure parse_csv_by_column_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN')
is
type string_ty   is table of varchar2 (4000) index by binary_integer;
l_column_header  string_ty;
l_bfile          bfile;
l_last           number := 1;
l_current        number;
l_string         varchar2 (32767);
l_next           varchar2 (100);
l_cnt            integer := 0;
l_first          boolean := TRUE;
--
function next_item return varchar2 
is
l_pos    integer;
l_return varchar2 (100);
  begin
	if substr(l_string, 1, 1) = ',' then l_string := substr (l_string, 2); end if;
	if substr (l_string, 1, 1) = '"'
	then 
	  l_pos    := instr  (l_string, '"', 2, 1);
	  l_return := substr (l_string, 2, l_pos - 2);
	else
	  l_pos    := instr  (l_string, ',');
	  l_return := substr (l_string, 1, l_pos - 1);
	end if;
      l_string := substr (l_string, l_pos + 1);	
	return l_return;
	
  exception when others then
    util.show_error ('Error in function next_item', sqlerrm);
    return null;
  end next_item;
--
begin
execute immediate 'truncate table gmn_csv_by_column_name';
l_bfile := bfilename (p_directory, p_csv_file);
dbms_lob.fileopen (l_bfile);

loop
  l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
  exit when nvl (l_current, 0) = 0;
  l_string := utl_raw.cast_to_varchar2 (dbms_lob.substr (l_bfile, l_current - l_last, l_last));

if l_first
then 
  loop
    l_cnt := l_cnt + 1;
    l_next    := next_item;
    l_column_header (l_cnt) := replace (l_next, 'session.');
    exit when nvl (instr (l_string, ','), 0) = 0;
  end loop;
  l_first := FALSE;
else
  for j in 1 .. l_cnt
  loop 
    l_next := next_item;
	insert into gmn_csv_by_column_name (file_id, field, val) values (p_file_id, l_column_header (j), l_next);
  end loop;
end if;
l_last := l_current + 1;
end loop;
dbms_lob.close (l_bfile);
commit;

exception when others then
    util.show_error ('Error in procedure parse_csv_by_column_name for file ID: ' || p_file_id, sqlerrm);
end parse_csv_by_column_name;

/******************************************************************************************************************************************************************/

--
-- Parse_csv_by_field_name
--
procedure parse_csv_by_field_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN', p_skip1 boolean default true)
is
l_bfile          bfile;
l_last           number := 1;
l_current        number;
l_string         varchar2 (32767);
l_next           varchar2 (1000);
l_cnt            integer := 0;
l_comma          integer;
l_rec1 varchar2 (1000);
l_rec2 varchar2 (1000);
l_rec3 varchar2 (1000);
--
function next_item return varchar2 
is
l_pos    integer;
l_return varchar2 (1000);
  begin
	if substr(l_string, 1, 1) = ',' then l_string := substr (l_string, 2); end if;
	if substr (l_string, 1, 1) = '"'
	then 
	  l_pos    := instr  (l_string, '"', 2, 1);
	  l_return := substr (l_string, 2, l_pos - 2);
	else
	  l_pos    := instr  (l_string, ',');
	  l_return := substr (l_string, 1, l_pos - 1);
	end if;
      l_string := substr (l_string, l_pos + 1);	
	return l_return;
	
  exception when others then
    util.show_error ('Error in function next_item', sqlerrm);
    return null;
  end next_item;
--
begin
execute immediate 'truncate table gmn_csv_by_field_name';
l_bfile  := bfilename (p_directory, p_csv_file);
dbms_lob.fileopen (l_bfile);

if p_skip1
then
  l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
  l_last := l_current + 1;
end if;

loop
  l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
  exit when nvl (l_current, 0) = 0;
  l_string := utl_raw.cast_to_varchar2 (dbms_lob.substr (l_bfile, l_current - l_last, l_last));

  loop
    l_rec1 := next_item;
    l_rec2 := next_item;
    l_rec3 := next_item;
    if l_rec1 not like '%Data%' and l_rec1 not like '%Definition%' and l_rec1 not like '%unknown%'  and l_rec1 not like 'Field%' and l_rec1 is not null
    then
      insert into gmn_csv_by_field_name (file_id, field, val, unit) values (p_file_id, l_rec1, l_rec2, l_rec3);
    end if;
	exit when nvl(instr (l_string, ','), 0) = 0;
  end loop;
  l_last := l_current + 1;
end loop;
commit;

exception when others then
    util.show_error ('Error in procedure parse_csv_by_field_name for file ID: ' || p_file_id, sqlerrm, true);
end parse_csv_by_field_name;

/******************************************************************************************************************************************************************/

--
-- load_session_info.
--
procedure load_session_info
is
l_column_list  varchar2 (2000);
l_file_list    varchar2 (2000);
l_csv_file     varchar2 (200);
cursor c_load_session (p_directory_path varchar2, p_file_type varchar2) is
  select fn.file_name, substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) fn, gf.id fit_id from table (get_file_name (p_directory_path || chr (92), p_file_type)) fn, gmn_fit_files gf
  where substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) = gf.file_name and gf.training is null;
begin
  set_nls;
  garmin_pkg.sync_fit_files;
  garmin_pkg.remove_csv_files;
  for dd in (select directory_name, directory_path from dba_directories where directory_name like 'FIT%')
  loop
    for ff in c_load_session (dd.directory_path, 'fit')
    loop
	  begin
        garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV-session.bat');
		l_csv_file := substr (ff.fn, 1, length (ff.fn) - 3) || 'csv';
		garmin_pkg.parse_csv_by_field_name (l_csv_file, ff.fit_id, dd.directory_name);
		l_column_list  := 'insert into gmn_session_info (fit_id, ';
        l_file_list    := '(' || to_char (ff.fit_id) || ', ';

        for j in (select distinct tc.column_name, tc.data_type, gc.unit, gc.val 
                  from gmn_csv_by_field_name gc, user_tab_columns tc where tc.table_name = 'GMN_SESSION_INFO'  and upper (gc.field) = tc.column_name)
        loop
          l_column_list := l_column_list || j.column_name  || ', ' ;
          if    j.unit      = 'semicircles' then l_file_list := l_file_list || ' garmin_pkg.semicircles_to_lon_lat (' || j.val || '), ';
          elsif j.data_type = 'NUMBER'      then l_file_list := l_file_list || j.val || ', ';
          elsif j.data_type = 'DATE'        then l_file_list := l_file_list || ' garmin_pkg.date_offset_to_date (' || j.val || '), ';
          else l_file_list := l_file_list || '''' || j.val || ''', ';
          end if; 
        end loop;
        l_column_list := rtrim (rtrim (l_column_list), ',') || ') values ';
        l_file_list   := rtrim (rtrim (l_file_list)  , ',') || ')';
  
        execute immediate l_column_list || l_file_list;
		update gmn_fit_files set training = 1 where id = ff.fit_id;
		commit;
	  exception when others then null;
	  end;
    end loop;
  end loop;  

exception when others then
    dbms_output.put_line (l_column_list);
	dbms_output.put_line (l_file_list);
    util.show_error ('Error in procedure load_session_info', sqlerrm, true);
end load_session_info;

/******************************************************************************************************************************************************************/

--
-- load_session_details
--
procedure load_session_details
is
l_statement   varchar2 (200);
l_prev_field  varchar2 (100) := '-1';
l_id integer (6) := 0;
l_csv_file     varchar2 (200);
cursor c_load_track (p_directory_path varchar2, p_file_type varchar2) is
  select fn.file_name, substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) fn, gf.id fit_id, user_id from table (get_file_name (p_directory_path || chr (92), p_file_type)) fn, gmn_fit_files gf
  where substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) = gf.file_name and gf.track is null;
begin
  set_nls;
  garmin_pkg.sync_fit_files;
  garmin_pkg.remove_csv_files;
  for dd in (select directory_name, directory_path from dba_directories where directory_name like 'FIT%')
  loop
    for ff in c_load_track (dd.directory_path, 'fit')
    loop
        garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV.bat');
		l_csv_file := substr (ff.fn, 1, length (ff.fn) - 3) || 'csv';
--		dbms_output.put_line (l_csv_file);
		garmin_pkg.parse_csv_by_field_name (l_csv_file, ff.fit_id, dd.directory_name);
        l_id := 1;
        for j in (select gc.id, tc.column_name, gc.field, gc.val, gc.unit, tc.data_type
                  from gmn_csv_by_field_name gc, user_tab_columns tc
                  where (tc.table_name = 'GMN_FIT_DATA'  and upper (gc.field) = tc.column_name)
                  order by gc.id)
        loop
          begin 
          if j.field = 'timestamp' and j.field != l_prev_field
          then
            l_id :=l_id + 1;
	        insert into gmn_fit_data (fit_id, id, person_id, "TIMESTAMP")  values (ff.fit_id, l_id, ff.user_id, garmin_pkg.date_offset_to_date (j.val));  
          elsif    j.unit      = 'semicircles'
		  then l_statement := 'update gmn_fit_data set ' || j.column_name || '='||  garmin_pkg.semicircles_to_lon_lat (j.val) || 
                                                                ' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || ff.user_id;
				execute immediate l_statement;
          elsif j.data_type = 'NUMBER'
		  then l_statement := 'update gmn_fit_data set ' || j.column_name || '='|| to_char(j.val) || 
                                                                ' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || ff.user_id;
			    execute immediate l_statement;
          elsif j.data_type = 'DATE'
		  then  l_statement := 'update gmn_fit_data set ' || j.column_name || '='''|| garmin_pkg.date_offset_to_date (j.val) || 
                                                                ''' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || ff.user_id;
				execute immediate l_statement;
          else l_statement := 'update gmn_fit_data set ' || j.column_name || '='''|| to_char(j.val) || 
                             ''' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || ff.user_id;
			   execute immediate l_statement;
          end if;  

		update gmn_fit_files set track = 1 where id = ff.fit_id;
		commit;
		
	  exception when others
	  then
	    dbms_output.put_line (l_statement);
		util.show_error ('Error in procedure load_session_details in inner loop', sqlerrm);
	  end;
	  l_prev_field := j.field;
	  end loop;
  end loop;
end loop;
delete gmn_fit_data where timestamp < to_date ('2000/01/01', 'YYYY/MM/DD');
commit;

exception when others then 
   util.show_error ('Error in procedure load_session_details', sqlerrm);
end load_session_details;


/******************************************************************************************************************************************************************/

--
-- Sync_fit_files
--
procedure sync_fit_files
is
begin
--  for dd in (select directory_path from dba_directories where directory_name like 'FIT%')
--  loop

  for n in (select id, first_name from gmn_users)
  loop
    insert into gmn_fit_files (user_id, file_name)
      select n.id, substr (file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Work\garmin\fit_' || n.first_name || chr(92), 'fit'))
	    where substr (file_name, instr (file_name, chr(92), 1, 4) + 1) not in (select file_name from gmn_fit_files) order by 1 desc;
  end loop;			
  commit;

exception when others then 
   util.show_error ('Error in procedure sync_fit_files', sqlerrm);
end sync_fit_files;

/******************************************************************************************************************************************************************/

--
-- Remove_csv_files from directories that contain the .fit files
--
procedure remove_csv_files
is
begin
  for n in (select first_name from gmn_users)
  loop
    for f in (select substr (file_name, instr (file_name, chr(92), 1, 4) + 1) fn from table (get_file_name ('C:\Work\garmin\fit_' || n.first_name || chr(92), 'csv')))
    loop
	  begin
        utl_file.fremove ('FIT_' || upper(n.first_name), f.fn);
	  exception when others
	  then
	    begin
	      utl_file.frename ('FIT_' || upper(n.first_name), f.fn, 'GARMIN_BACKUP', f.fn || '_' || to_char (sysdate, 'YYYYMonDD'), true);
		exception when others then null;
		end;
	  end;
    end loop;
  end loop;

exception when others then 
   util.show_error ('Error in procedure remove_csv_files', sqlerrm);
end remove_csv_files;

/******************************************************************************************************************************************************************/

--
-- Returning corresponding range heartrate for a person: 1 = low, 2 = medium, 3 = high
--
function get_heartrate (p_person_id in integer, p_range in integer) return integer
is
l_heartrate number (3);
begin
  select case p_range when 1 then hr_low when 2 then hr_medium when 3 then hr_high else null end into l_heartrate from gmn_users where id = p_range;
  return l_heartrate;

exception when others then 
   util.show_error ('Error in function get_heartrate for userid: ' || p_person_id || ' and range: ' || p_range, sqlerrm);
   return null;
end get_heartrate;

/******************************************************************************************************************************************************************/

--
-- Checks if session details are present
--
function fit_data_loaded (p_fit_id in integer) return integer
is
l_ok integer (1);
begin
  select 1 into l_ok from gmn_fit_data where fit_id = p_fit_id and rownum = 1;
  return l_ok;

exception when no_data_found 
then return 0;
when others then 
   util.show_error ('Error in function fit_data_loaded for fit_id: ' || p_fit_id, sqlerrm);
   return null;
end fit_data_loaded;

/******************************************************************************************************************************************************************/

--
-- Reload fit data for a file
--
procedure reload_fit_data (p_fit_id in integer)
is
begin
  update gmn_fit_files set training = null, track = null where id = p_fit_id;
  delete gmn_session_info where fit_id = p_fit_id;
  delete gmn_fit_data     where fit_id = p_fit_id;
  garmin_pkg.load_session_info;
  garmin_pkg.load_session_details;

exception when others then 
   util.show_error ('Error in procedure reload_fit_data for ID: ' || p_fit_id, sqlerrm);
end reload_fit_data;

/******************************************************************************************************************************************************************/

--
-- Analytics pivot function
--
function analyze_data (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 40) return text_tab pipelined
is 
type cell_ty       is record (cell varchar2 (50));
type cell_row_ty   is table of cell_ty index by pls_integer;
type cell_field_ty is table of cell_row_ty index by pls_integer;
l_cells            cell_field_ty;
l_row              integer (4) := 0;
l_max_laps         integer (2) := 1;
begin 
for y in 0 .. p_measurements
loop
  l_cells (0) (y).cell := to_char (y);
  for x in 1 .. 50
  loop
    l_cells (x) (y).cell := null;
  end loop;
end loop;
l_cells (0) (0).cell := p_sport_profile;

for d in (select ff.user_id, si.fit_id, si.sport_profile_name, trunc (si.start_time) start_time from gmn_session_info si, gmn_fit_files ff
          where ff.id = si.fit_id and si.sport_profile_name = p_sport_profile and ff.user_id = p_person_id
          order by si.start_time desc
          fetch first p_measurements rows only)
loop
  l_row := l_row + 1;
  l_cells (l_row) (0).cell := to_char (d.start_time, 'YYYY/MM/DD');
  for j in (select rownum lap,avg_heart_rate, total_distance, round(3.6 *  enhanced_avg_speed, 2) enhanced_avg_speed, total_timer_time,
  case enhanced_avg_speed when 0 then null else substr (garmin_pkg.seconds_to_ds_interval (round(1000 / enhanced_avg_speed)), 15, 8) end tempo, total_calories, round (3.6 * enhanced_max_speed, 2) enhanced_max_speed, total_ascent, 
			  total_descent, enhanced_max_altitude
            from gmn_fit_data gf
           where gf.fit_id= d.fit_id and gf.start_position_lat is not null and gf.total_training_effect is null
           order by id)
  loop 
    case
	when p_field = 1  then l_cells (l_row) (j.lap).cell := to_char (j.avg_heart_rate);
	when p_field = 2  then l_cells (l_row) (j.lap).cell := to_char (round (j.total_distance));
	when p_field = 3  then l_cells (l_row) (j.lap).cell := to_char (j.enhanced_avg_speed, '990D99');
	when p_field = 4  then l_cells (l_row) (j.lap).cell := to_char (round (j.total_timer_time)); 
	when p_field = 5  then l_cells (l_row) (j.lap).cell := to_char (j.tempo);
	when p_field = 6  then l_cells (l_row) (j.lap).cell := to_char (j.total_calories); 
	when p_field = 7  then l_cells (l_row) (j.lap).cell := to_char (to_char (j.enhanced_max_speed, '990D99')); 
	when p_field = 8  then l_cells (l_row) (j.lap).cell := to_char (j.total_ascent); 
	when p_field = 9  then l_cells (l_row) (j.lap).cell := to_char (j.total_descent);
	when p_field = 10 then l_cells (l_row) (j.lap).cell := to_char (j.enhanced_max_altitude, '990D99');
    end case;
	l_max_laps := greatest (l_max_laps, j.lap);
  end loop;
end loop;

for y in 0 .. l_max_laps
loop
begin
  pipe row (text_row (l_cells (0) (y).cell, l_cells (1) (y).cell, l_cells (2) (y).cell, l_cells (3) (y).cell, l_cells (4) (y).cell, l_cells (5) (y).cell,
            l_cells (6) (y).cell, l_cells (7) (y).cell, l_cells (8) (y).cell, l_cells (9) (y).cell, l_cells (10) (y).cell,
			l_cells (11) (y).cell, l_cells (12) (y).cell, l_cells (13) (y).cell, l_cells (14) (y).cell, l_cells (15) (y).cell,
			l_cells (16) (y).cell, l_cells (17) (y).cell, l_cells (18) (y).cell, l_cells (19) (y).cell, l_cells (20) (y).cell));
exception when others then null;
end;
end loop;

exception when others then 
   util.show_error ('Error in function analyze_data for person_id: ' || p_person_id || ',  Field: ' || p_field || '. Sport: ' || p_sport_profile, sqlerrm);
end analyze_data;

/******************************************************************************************************************************************************************/

--
-- Analytics function
--
function analyze_graph (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 20) return int_tab pipelined
is 
type cell_ty       is record (cell number (10, 2));
type cell_row_ty   is table of cell_ty     index by pls_integer;
type cell_field_ty is table of cell_row_ty index by pls_integer;
type train_date_ty is table of date        index by pls_integer;
l_cells            cell_field_ty;
l_row              integer (4) := 0;
l_train_dates      train_date_ty;
begin 
for y in 1 .. p_measurements
loop
--  l_cells (0) (y).cell := y;
  for x in 1 .. 50
  loop
    l_cells (x) (y).cell := null;
  end loop;
end loop;

for d in (select ff.user_id, si.fit_id, si.sport_profile_name, trunc (si.start_time) start_time from gmn_session_info si, gmn_fit_files ff
          where ff.id = si.fit_id and si.sport_profile_name = p_sport_profile and ff.user_id = p_person_id
          order by si.start_time desc
          fetch first p_measurements rows only)
loop
  l_row := l_row + 1;
  l_train_dates (l_row) := d.start_time;
  for j in (select rownum lap,avg_heart_rate, total_distance, round(3.6 *  enhanced_avg_speed, 2) enhanced_avg_speed, total_timer_time,
  case enhanced_avg_speed when 0 then null else round (1000 / enhanced_avg_speed) end tempo, total_calories, round (3.6 * enhanced_max_speed, 2) enhanced_max_speed, total_ascent, 
			  total_descent, enhanced_max_altitude
            from gmn_fit_data gf
           where gf.fit_id= d.fit_id and gf.start_position_lat is not null and gf.total_training_effect is null
           order by id)
  loop
    case
	when p_field = 1  then l_cells (j.lap) (l_row).cell := j.avg_heart_rate;
	when p_field = 2  then l_cells (j.lap) (l_row).cell := round (j.total_distance);
	when p_field = 3  then l_cells (j.lap) (l_row).cell := round (j.enhanced_avg_speed, 2);
	when p_field = 4  then l_cells (j.lap) (l_row).cell := round (j.total_timer_time); 
	when p_field = 5  then l_cells (j.lap) (l_row).cell := j.tempo;
	when p_field = 6  then l_cells (j.lap) (l_row).cell := j.total_calories; 
	when p_field = 7  then l_cells (j.lap) (l_row).cell := round (j.enhanced_max_speed, 2); 
	when p_field = 8  then l_cells (j.lap) (l_row).cell := j.total_ascent; 
	when p_field = 9  then l_cells (j.lap) (l_row).cell := j.total_descent;
	when p_field = 10 then l_cells (j.lap) (l_row).cell := j.enhanced_max_altitude;
    end case;
  end loop;
end loop;

for y in 0 .. p_measurements
loop
begin
  pipe row (int_row (l_train_dates (y), l_cells (1) (y).cell, l_cells (2) (y).cell, l_cells (3) (y).cell, l_cells (4) (y).cell, l_cells (5) (y).cell,
            l_cells (6) (y).cell, l_cells (7) (y).cell, l_cells (8) (y).cell, l_cells (9) (y).cell, l_cells (10) (y).cell,
			l_cells (11) (y).cell, l_cells (12) (y).cell, l_cells (13) (y).cell, l_cells (14) (y).cell, l_cells (15) (y).cell,
			l_cells (16) (y).cell, l_cells (17) (y).cell, l_cells (18) (y).cell, l_cells (19) (y).cell, l_cells (20) (y).cell));
exception when others then null;
end;
end loop;

exception when others then 
   util.show_error ('Error in function analyze_graph for person_id: ' || p_person_id || ',  Field: ' || p_field || '. Sport: ' || p_sport_profile, sqlerrm);
end analyze_graph;

end garmin_pkg;
/



/* Demo


select id, first_name from gmn_users
select directory_name, directory_path from dba_directories where directory_name like 'FIT%'

select substr (file_name, instr (file_name, chr(92), 1, 4) + 1) fn from table (get_file_name (directory_path || chr (92), 'fit'));

declare
l_file varchar2 (200) := 'C:\Work\garmin\fit_dolly\2023-10-02-10-20-34.fit';
begin
  garmin_pkg.remove_csv_files;
  garmin_pkg.sync_fit_files;
  garmin_pkg.convert_fit_file_to_csv (l_file, 'FitToCSV-session.bat');
  for f in (select substr (file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Work\garmin\fit_dolly' || chr(92), 'csv')))
  loop
    if f.fn like '%data.csv'
	then garmin_pkg.parse_csv_by_column_name (f.fn, 123, 'FIT_DOLLY');
	else garmin_pkg.parse_csv_by_field_name (f.fn, 123, 'FIT_DOLLY');
	end if;
  end loop;
end;
/

declare
l_csv_file  varchar2 (200);
cursor c_files (p_directory_path varchar2, p_file_type varchar2) is
  select fn.file_name, substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) fn, gf.id fit_id from table (get_file_name (p_directory_path || chr (92), p_file_type)) fn, gmn_fit_files gf
  where substr (fn.file_name, instr (fn.file_name, chr(92), 1, 4) + 1) = gf.file_name and gf.lap is null and rownum =1;
begin
  garmin_pkg.sync_fit_files;
  garmin_pkg.remove_csv_files;
  for dd in (select directory_name, directory_path from dba_directories where directory_name like 'FIT%')
  loop
    for ff in c_files (dd.directory_path, 'fit')
    loop
	  begin
        garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV.bat');
		l_csv_file := substr (ff.fn, 1, length (ff.fn) - 3) || 'csv';
		dbms_output.put_line (l_csv_file);
		garmin_pkg.parse_csv_by_field_name (l_csv_file, ff.fit_id, dd.directory_name);
--		garmin_pkg.load_session_info (ff.fit_id);
--		update gmn_fit_files set training = 1 where id = ff.fit_id;
		commit;
	  exception when others then null;
	  end;
    end loop;
  end loop;
end;
/





case
when gc.unit = 'semicircles' then to_char(garmin_pkg.semicircles_to_lon_lat (gc.val))
when tc.data_type = 'DATE'  then to_char (garmin_pkg.date_offset_to_date (val))
 else val end rrr
f;
 
FitToCSV-session.bat 2 files:  F  2023-08-29-11-13-10.csv                                              !! C 2023-08-29-11-13-10_data.csv duplicate
!!  FitToCSV-record.bat  2 files F2023-08-29-11-13-10.csv C 2023-08-29-11-13-10_data.csv   Skip for now
FitToCSV-lap.bat  2 files: F 2023-08-29-11-13-10.csv C 2023-08-29-11-13-10_data.csv
FitToCSV.bat  F 1 2023-08-29-11-13-10.csv



FitToCSV-data.bat !! C not usefull



exec garmin_pkg.parse_csv_by_field_name ('2023-08-29-11-13-10.csv', 84, 'FIT_DOLLY', 4)

   
FitToCSV-session.bat: Fields

---

select column_name from user_tab_columns where table_name = 'GMN_SESSION_INFO';


FitToCSV-record.bat:
timestamp
activity_type
cadence
distance
enhanced_altitude
enhanced_speed
fractional_cadence
heart_rate
position_lat
position_long
power
accumulated_power
cycle_length16
stance_time
step_length
vertical_oscillation
vertical_ratio

copy /B  /Y T"\\fenix 7\Internal Storage\GARMIN\Activity\*.fit"  C:\Users\FolderPath\Desktop\test

----- */




insert into gmn_session_info (
AVG_HEART_RATE,
MAX_HEART_RATE,
ENHANCED_AVG_SPEED,
ENHANCED_MAX_SPEED,
SPORT,
SPORT_PROFILE_NAME,
START_TIME,
TOTAL_TRAINING_EFFECT,
TOTAL_ASCENT,
TOTAL_DESCENT,
TOTAL_DISTANCE,
TOTAL_CALORIES,
TOTAL_ELAPSED_TIME,
AVG_POWER,
AVG_STEP_LENGTH,
MAX_POWER,
NORMALIZED_POWER)
select 
GEM_HS,	
MAX_HS,	
GEMIDDELDE_SNELHEID	,
MAX_SNELHEID,	
- id	,
ACTIVITEITTYPE	,
DATUM	,
AEROOB_TE	,
TOTALE_STIJGING	,
TOTALE_DALING	,
AFSTAND	,
CALORIEEN	,
garmin_pkg.interval_ds_to_seconds (TIJD_BEWOGEN)	,
GEM_VERMOGEN	,
GEM_STAPLENGTE	,
MAX_VERMOGEN	,
NORMALIZED_POWER
from garmin_old
where activiteittype = 'Hardlopen';
and to_char(datum , 'YYYY/MM/DD')  not in (selec to_char (start_time, 'YYYY/MM/DD')  from gmn_session_info where activiteittype = 'Hardlopen')

