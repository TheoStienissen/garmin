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
2. Pipelined function get_file_name



#

drop table garmin_details;
drop table garmin cascade constraints;
drop table garmin_users;
drop sequence garmin_seq;

-- The avatar or nick_name needs to be unique
create table garmin_users
( id            integer generated always as identity
, first_name    varchar2 (20)
, last_name     varchar2 (20)
, windows_account   varchar2 (20)
, nick_name     varchar2 (20) not null
, hr_low        number (3)    not null default 100
, hr_medium     number (3)    not null default 120
, hr_high       number (3)    not null default 140);

insert into garmin_users (first_name, last_name, nick_name) values ('Theo', 'Stienissen', 'Theo');
insert into garmin_users (first_name, last_name, nick_name) values ('Dolly', 'Stienissen', 'Dolly');
insert into garmin_users (first_name, last_name, nick_name) values ('Celeste', 'Stienissen', 'Celeste');
alter table garmin_users add constraint garmin_users_pk primary key (id);
create unique index garmin_users_uk1 on garmin_users (nick_name);

create table garmin (
id                         integer,
person_id                  integer (4),
Activiteittype             varchar2 (50),
Datum                      date,
Favoriet                   varchar2 (5),
Titel                      varchar2 (50),
Afstand                    number (5, 2),
Calorieen                  number (6),
Tijd                       interval day to second,
Gem_HS                     number (3),
Max_HS                     number (3),
Aeroob_TE                  number (2,1),
Gemiddelde_fietscadans     varchar2 (10),
Max_fietscadans            varchar2 (10),
Gemiddelde_snelheid        number (5, 2),
Max_snelheid               number (5, 2),
Totale_stijging            number (4),
Totale_daling              number (4),
Gem_staplengte             number (4,2),
Gemiddelde_verticale_ratio number (4,2),
Gem_verticale_oscillatie   number (4,2),
Gem_grondcontacttijd       number (6, 2),
Gem_GAP                    varchar2 (10),
Training_Stress_Score      number (6,2),
Gem_vermogen               number (6,2),
Max_vermogen               number (6,2),
Grit                       number (6,2),
Flow                       number (6,2),
Gemiddelde_Swolf           number (4,2),
Gem_slagsnelheid           number (4,2),
Totaal_herhalingen         number (3),
Totaal_sets                number (3),
Duiktijd                   varchar2 (10),
Min_temp                   number (3,1),
Oppervlakte_interval       varchar2 (10),
Decompressie               varchar2 (10),
Beste_rondetijd            interval day to second,
Aantal_ronden              number (3),
Max_temp                   number (3,1),
Tijd_bewogen               interval day to second,
Verstreken_tijd            interval day to second,
normalized_power           number (6,2)
Minimum_hoogte             number (4),
Maximum_hoogte             number (4),
gem_cadans                 number (5,2),
gem_loopcadans             number (5,2),
max_loopcadans             number (5,2),
Maximale_cadans            number (5,2),
Gemiddeld_tempo            interval day to second,
Beste_tempo                interval day to second);


alter table garmin add constraint garmin_pk primary key (id);
create unique index garmin_uk1 on garmin (person_id, Activiteittype, Datum);
alter table garmin add constraint garmin_fk12 foreign key (person_id) references garmin_users (id);
create sequence garmin_seq;

create table garmin_details (
garmin_id                  number (10),
Ronde                      number (4),
Tijd                       interval day to second,
Totale_tijd	               interval day to second,
Afstand                    number (5, 2),
Gemiddelde_snelheid        number (4, 1),
Gem_GAP                    varchar2 (10),
Gem_HS                     number (3),
Max_HS                     number (3),
Totale_stijging            number (4),
Totale_daling              number (4),
Calorieen                  number (6),
Max_snelheid               number (4, 1),
Tijd_bewogen	           interval day to second,
Gem_bewogen_snelheid       number (4, 1),
Gem_vermogen               number (6,2),
gem_tempo    	           interval day to second,
watt_per_kg                number (5, 2),
max_watt_per_kg            number (5, 2),
max_vermogen               number (6,2),
gem_loopcadans             number (5, 2),
Gem_grondcontacttijd       number (6, 2),
gem_gct_balans             varchar2 (10),
Gem_staplengte             number (4,2),
Gemiddelde_verticale_ratio number (4,2),
Gem_verticale_oscillatie   number (4,2),
gem_temperatuur            number (4,2),
Beste_tempo                interval day to second,
gem_bewogen_tempo          interval day to second,
max_loopcadans             number (5,2)
interval                   number (3)
staptype                   varchar2 (10));

alter table garmin_details add constraint garmin_datails_pk primary key (garmin_id, Ronden);
alter table garmin_details add constraint garmin_details_fk1 foreign key (garmin_id) references garmin (id) on delete cascade;

drop table garmin_missing_columns;
create table garmin_missing_columns
( id                      integer generated always as identity
, table_name              varchar2 (30)
, column_name             varchar2 (30)
, pk1                     number (10)
, pk2                     number (10)
, val                     varchar2 (20));

create directory garmin as 'C:\Work\garmin';
create directory garmin_backup as 'C:\Work\garmin\backup';
create or replace directory Downloads_Dolly as 'C:\Users\Dolly\Downloads';
create or replace directory Downloads_Theo as 'C:\Users\Theo\Downloads';

create or replace directory fit_dolly as 'C:\Work\garmin\fit_dolly';
create or replace directory fit_theo as 'C:\Work\garmin\fit_theo';

-- 5. Create a table with all the attribute fields that we are interested in
create table garmin_fit_data
( garmin_id            number (6)
, id                   number (10)
, person_id            number (4)
, avg_heart_rate       number (3)
, avg_power            number (3)
, max_power            number (3)
, step_length          number (5, 1)
, avg_step_length      number (5, 1)
, distance             number (7, 2)
, total_distance       number (7, 2)
, enhanced_avg_speed   number (6, 3)
, enhanced_speed       number (6, 3)
, heart_rate           number (3)
, max_heart_rate       number (3)
, num_laps             number (2)
, position_lat         number
, position_long        number
, power                number (3)
, start_position_lat   number
, start_position_long  number
, start_time           date
, timestamp            date
, total_calories       number (4)
, total_elapsed_time   number (9, 3)
, total_training_effect number (2, 1)
, total_work           number (7))
 PARTITION BY list (garmin_id) automatic
(PARTITION P_1 VALUES (1));

alter table garmin_fit_data add constraint garmin_fit_data_pk primary key (garmin_id, id, person_id);
alter table garmin_fit_data add constraint garmin_fit_data_fk1 foreign key (person_id) references garmin_users (id);

create table garmin_map_fit_file_to_id
( garmin_id          number (10)
, person_id          number (4)
, file_name          varchar2 (30)
, loaded             number (1));

alter table garmin_map_fit_file_to_id add constraint garmin_map_fit_file_to_id_pk primary key (file_name);
create unique index garmin_map_fit_file_to_id_uk1 on garmin_map_fit_file_to_id (garmin_id);
alter table garmin_map_fit_file_to_id add constraint garmin_map_fit_file_to_id_fk1 foreign key (person_id) references garmin_users (id);

set serveroutput on size unlimited

create or replace package garmin_pkg
is
g_max_int   constant integer := power (2, 31);
g_username  constant varchar2 (10) := 'THEO';        -- Database schema user
g_password  constant varchar2 (10) := 'Celeste14';   -- Database password
g_connect_string constant varchar2 (10) := 'db19c';  -- Database tns entry

g_user_id   number (10);
g_garmin_id number (10);

function ds_to_varchar2 (p_ds in interval day to second) return varchar2;

function to_nr (p_string in varchar2, p_col in varchar2 default null) return number;

function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second;

function to_dt (p_string in varchar2, p_col in varchar2 default null) return date;

procedure load_garmin_data (p_nick_name in varchar2, p_garmin_id in integer default null);

procedure move_csv_files_to_backup;

procedure move_from_downloads_to_garmin (p_nick_name in varchar2 default null);

procedure load_csv_file (p_nick_name in varchar2, p_garmin_id in integer default null, p_remove in integer default 1);

function interval_ds_to_seconds (p_interval in interval day to second) return integer;

function seconds_to_ds_interval (p_seconds in integer) return interval day to second;

function date_offset_to_date (p_offset in integer) return date;

function semicircles_to_lon_lat (p_semicircle in integer) return number;

procedure convert_fit_file_to_csv (p_file_name in varchar2);

procedure create_sqlldr_control_file (p_file_name in varchar2);

procedure load_converted_fit_file_with_sqlldr;

procedure extract_detailed_data (p_garmin_id in integer, p_person_id in number);

procedure map_fit_files_to_garmin_id;

procedure start_load_fit_data (p_nick_name in varchar2 default null);

procedure end_load_fit_data (p_remove_csv in boolean default true);

function get_heartrate (p_person_id in integer, p_range in integer) return integer;

end garmin_pkg;
/


create or replace package body garmin_pkg
is

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
  then l_nr := round (3600 / (60 * substr (p_string, 1, instr (p_string, ':') - 1) + substr (p_string, instr (p_string, ':') + 1)), 2);  
  else l_nr := to_number (replace (p_string, ',', '.'));  
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
--  dbms_output.put_line ('to_ds: ' || p_col || '   ' || p_string);
  if     p_string is null or p_string = '--' then l_ds := null;
  elsif  instr (p_string, ':') = 0           then l_ds :=  to_dsinterval ( '00 00:00:' || replace (p_string, ',', '.'));
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
-- Load the data from the activities.csv file
--
procedure load_garmin_data (p_nick_name in varchar2, p_garmin_id in integer default null)
is 
  type string_ty   is table of varchar2 (4000) index by binary_integer;
  l_column_header              string_ty;
  l_lines                      string_ty;
  l_fhandle                    utl_file.file_type;
  l_activiteit                 varchar2 (50);
  l_datum                      date;
  l_string                     varchar2 (4000);
  l_next                       varchar2 (50);
  l_first                      varchar2 (500);
  l_pos                        number (4);
  l_return                     varchar2 (50);
  l_persoon                    varchar2 (20);
  l_count                      integer (6);
  l_id                         integer;
  l_ronde                      integer (4);
  l_next_nr                    number (5, 2);
  l_next_ds                    interval day to second;
--
  function next_item return varchar2 
  is 
  begin
    l_string := ltrim (l_string, ',');
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
  utl_file.fclose_all;
  for f in (select u.id, substr(fn.file_name, instr (fn.file_name, chr(92), 1, 3) +1) fn from table (get_file_name ('C:\Work\garmin' || chr(92), 'csv')) fn, garmin_users u
                    where upper(fn.file_name) like '%ACTIVIT%' and u.nick_name = p_nick_name order by file_name)
  loop 
  begin 
    l_fhandle := utl_file.fopen ('GARMIN' , f.fn, 'r' );
    utl_file.get_line (l_fhandle, l_string);
	l_column_header.delete;
	l_count := 1;
	l_first := next_item;
    l_column_header (1) := l_first;
	loop 
    l_next := next_item;
    exit when l_next is null;
--	dbms_output.put_line(l_next);
	l_count := l_count + 1;
	l_column_header (l_count) := l_next;
	end loop;
	
  if l_first = 'Activiteittype' and f.fn = 'Activities.csv'
  then 
 	  loop
	  begin 
		utl_file.get_line (l_fhandle, l_string);
		l_activiteit := next_item;
		l_datum      := to_dt (next_item);
        select garmin_seq.nextval into l_id from dual;
		insert into garmin (id, person_id, Activiteittype, Datum) values (l_id, f.id, l_activiteit, l_datum);
		for i in 3 .. l_count
		loop 
		  l_next := next_item;
		  dbms_output.put_line (to_char (i)  || ';   ' || l_column_header (i) || ':' || l_next);
		  case l_column_header (i) 
		  when 'Favoriet'                then update garmin set Favoriet = l_next                                 where id = l_id;
		  when 'Titel'                   then update garmin set Titel = l_next                                    where id = l_id;
		  when 'Afstand'                 then update garmin set Afstand = to_nr (l_next, 'Afstand')               where id = l_id;		  	  
		  when 'Calorieδn'               then update garmin set Calorieen = to_nr (l_next, 'Calorieen')           where id = l_id;	
		  when 'Tijd'                    then update garmin set Tijd = to_ds (l_next, 'Tijd')                     where id = l_id;
		  when 'Gem. HS'                 then update garmin set Gem_Hs = to_nr (l_next, 'Gem_HS')                 where id = l_id;		  
		  when 'Max. HS'                 then update garmin set Max_Hs = to_nr (l_next, 'Max_HS')                 where id = l_id;		  
		  when 'Aeroob TE'               then update garmin set Aeroob_TE = to_nr (l_next, 'Aeroob_TE')           where id = l_id;		  
		  when 'Gemiddelde fietscadans'  then update garmin set Gemiddelde_fietscadans = l_next                   where id = l_id;		  
		  when 'Max. fietscadans'        then update garmin set Max_fietscadans = l_next                          where id = l_id;  
		  when 'Gemiddelde snelheid'     then update garmin set Gemiddelde_snelheid = to_nr (l_next, 'Gemiddelde_snelheid') where id = l_id;
		  when 'Max. snelheid'           then update garmin set Max_snelheid = to_nr (l_next, 'Max_snelheid')     where id = l_id;
		  when 'Totale stijging'         then update garmin set Totale_stijging = to_nr (l_next, 'Totale_stijging') where id = l_id;		  
		  when 'Totale daling'           then update garmin set Totale_daling = to_nr (l_next, 'Totale_daling')   where id = l_id;
		  when 'Gem. staplengte'         then update garmin set Gem_staplengte = to_nr (l_next, 'Gem_staplengte') where id = l_id;		  
		  when 'Gemiddelde verticale ratio' then update garmin set Gemiddelde_verticale_ratio = to_nr (l_next, 'Gemiddelde_verticale_ratio') where id = l_id;	  
		  when 'Gem. verticale oscillatie'  then update garmin set Gem_verticale_oscillatie = to_nr (l_next, 'Gem_verticale_oscillatie') where id = l_id;
		  when 'Gem. grondcontacttijd'   then update garmin set Gem_grondcontacttijd = to_nr (l_next, 'Gem_grondcontacttijd') where id = l_id;
		  when 'Gem. GAP'                then update garmin set Gem_GAP = l_next                                   where id = l_id;
		  when 'Training Stress Score«'  then update garmin set Training_Stress_Score = to_nr (l_next, 'Training_Stress_Score') where id = l_id;
		  when 'Gem. vermogen'           then update garmin set Gem_vermogen = to_nr (l_next, 'Gem_vermogen')      where id = l_id;
		  when 'Max. vermogen'           then update garmin set Max_vermogen = to_nr (l_next, 'Max_vermogen')      where id = l_id;
		  when 'Grit'                    then update garmin set Grit = to_nr (l_next, 'Grit')                      where id = l_id;
		  when 'Flow'                    then update garmin set Flow = to_nr (l_next, 'Flow')                      where id = l_id;
		  when 'Gemiddelde Swolf'        then update garmin set Gemiddelde_Swolf = to_nr (l_next, 'Gemiddelde_Swolf') where id = l_id;
		  when 'Gem. slagsnelheid'       then update garmin set Gem_slagsnelheid = to_nr (l_next, 'Gem_slagsnelheid') where id = l_id; 
		  when 'Totaal herhalingen'      then update garmin set Totaal_herhalingen = to_nr (l_next, 'Totaal_herhalingen') where id = l_id; 
		  when 'Duiktijd'                then update garmin set Duiktijd = l_next                                   where id = l_id; 
		  when 'Totaal sets'             then update garmin set Totaal_sets = to_nr (l_next, 'Totaal sets')         where id = l_id;
		  when 'Min. temp.'              then update garmin set Min_temp = to_nr (l_next, 'Min_temp')               where id = l_id; 
		  when 'Oppervlakte-interval'    then update garmin set Oppervlakte_interval = l_next                       where id = l_id; 
		  when 'Decompressie'            then update garmin set Decompressie = l_next                               where id = l_id;
		  when 'Beste rondetijd'         then update garmin set Beste_rondetijd = to_ds (l_next, 'Beste_rondetijd') where id = l_id;
		  when 'Aantal ronden'           then update garmin set Aantal_ronden = to_nr (l_next, 'Aantal_ronden')     where id = l_id;
		  when 'Max. temp.'              then update garmin set Max_temp = to_nr (l_next, 'Max_temp')               where id = l_id; 
		  when 'Tijd bewogen'            then update garmin set Tijd_bewogen = to_ds (l_next, 'Tijd_bewogen')       where id = l_id; 
		  when 'Verstreken tijd'         then update garmin set Verstreken_tijd = to_ds (l_next, 'Verstreken_tijd') where id = l_id;  
		  when 'Minimum hoogte'          then update garmin set Minimum_hoogte = to_nr (l_next, 'Minimum_hoogte')   where id = l_id;
		  when 'Maximum hoogte'          then update garmin set Maximum_hoogte = to_nr (l_next, 'Maximum_hoogte')   where id = l_id; 
		  when 'Gem. cadans'             then update garmin set Gem_cadans = to_nr (l_next, 'Gem. cadans')          where id = l_id;
		  when 'Gem. loopcadans'         then update garmin set Gem_loopcadans = to_nr (l_next, 'Gem. loopcadans')  where id = l_id;
		  when 'Max. loopcadans'         then update garmin set max_loopcadans = to_nr (l_next, 'Max. loopcadans')  where id = l_id;
		  when 'Maximale cadans'         then update garmin set Maximale_cadans = to_nr (l_next, 'Maximale cadans') where id = l_id;
		  when 'Gemiddeld tempo'         then update garmin set Gemiddeld_tempo = to_ds (l_next, 'Gemiddeld tempo') where id = l_id;
		  when 'Beste tempo'             then update garmin set Beste_tempo = to_ds (l_next, 'Beste tempo')         where id = l_id;			  
		  else
           if l_column_header (i) Like 'Normalized Power%'
           then update garmin set normalized_power = to_nr (l_next, 'Normalized power') where id = l_id;
           else
		     insert into garmin_missing_columns (table_name, column_name, pk1, val) values ('GARMIN', l_column_header (i), l_id, l_next);   
           end if;
		  end case;
        end loop;		  

  exception 
  when dup_val_on_index then null;  
  when no_data_found
  then utl_file.fclose (l_fhandle);
    exit;
  end;
  end loop;
  
  elsif f.fn like 'activity%'
  then 
		l_fhandle := utl_file.fopen ('GARMIN' , f.fn, 'r' );
		l_count := 0;
		l_column_header.delete;
		l_lines.delete;
		loop -- Load all lines from the csv.
		  begin 
		    utl_file.get_line (l_fhandle, l_string);
			l_count := l_count + 1;
	        l_lines (l_count) := l_string;
		  exception 
		  when no_data_found then utl_file.fclose (l_fhandle); exit;
		  end;
		end loop;
		
		l_next    := next_item; -- [Overzicht] Last line
		while l_next not like '%:%' or l_next is null -- Find the first time ds field
		loop 
		  l_next    := next_item;
--		  dbms_output.put_line('In second part loop: ' || l_next);
		end loop;
		l_next_ds := to_ds (l_next);
		
		
		if p_garmin_id is null
		then select max (id) into l_id from garmin g where tijd  = l_next_ds and not exists (select null from garmin_details d where d.garmin_id = g.id);
		else l_id := p_garmin_id; delete garmin_details where garmin_id = p_garmin_id;
		end if;
		
		if 	l_id is not null
		then 
          l_count  := 0;
	      l_string := l_lines (1);
	      loop 
            l_next := next_item;
            exit when l_next is null and l_count != 0;
	          l_count := l_count + 1;
--            dbms_output.put_line(to_char(l_count) || ':  ' || l_next);
	          l_column_header (l_count) := l_next;
	      end loop;	
	
		for i in 2 .. l_lines.count - 1
		loop 
	      l_string := l_lines (i);
          l_ronde  := i - 1;          --  to_nr (next_item, 'Ronde');
          insert into garmin_details (garmin_id, ronde) values (l_id, l_ronde);
		  for c in 1 .. l_column_header.count
		  loop 
--		    select dump(l_column_header (c)) into l_first from dual;
            l_next := next_item;
			dbms_output.put_line(to_char (c)  || ';   ' || l_column_header (c) || ':' || l_next);
	  	    case l_column_header (c)
	        when 'Tijd'                       then update garmin_details set Tijd = to_ds (l_next, 'Tijd')                                  where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Totale tijd'                then update garmin_details set Totale_tijd = to_ds (l_next, 'Totale tijd')                    where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Afstand'                    then update garmin_details set Afstand = to_nr (l_next, 'Afstand')                            where garmin_id = l_id and ronde = l_ronde;
	        when 'Gem. vermogen'              then update garmin_details set gem_vermogen = to_nr (l_next, 'Gem. vermogen')                 where garmin_id = l_id and ronde = l_ronde;		
			when 'Gemiddeld tempo'            then update garmin_details set gem_tempo = to_ds  (l_next, 'Gemiddeld tempo')                 where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. W/kg'                  then update garmin_details set watt_per_kg = to_nr (l_next, 'Gem. W/kg')                      where garmin_id = l_id and ronde = l_ronde;	  
			when 'Max. vermogen'              then update garmin_details set max_vermogen = to_nr (l_next, 'Max. vermogen')                 where garmin_id = l_id and ronde = l_ronde;	  
			when 'Max. W/kg'                  then update garmin_details set max_watt_per_kg = to_nr (l_next, 'Max. W/kg')                  where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. loopcadans'            then update garmin_details set gem_loopcadans = to_nr (l_next, 'Gem. loopcadans')             where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. grondcontacttijd'      then update garmin_details set gem_grondcontacttijd = to_nr (l_next, 'Gem. grondcontacttijd') where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. GCT balans'            then update garmin_details set gem_gct_balans = l_next                                        where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. staplengte'            then update garmin_details set Gem_staplengte = to_nr (l_next, 'Gem. staplengte')             where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. verticale oscillatie'  then update garmin_details set Gem_verticale_oscillatie = to_nr (l_next, 'Gem. verticale oscillatie') where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gemiddelde verticale ratio' then update garmin_details set Gemiddelde_verticale_ratio = to_nr (l_next, 'Gemiddelde verticale ratio') where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gem. temperatuur'           then update garmin_details set gem_temperatuur = to_nr (l_next, 'Gem. temperatuur')           where garmin_id = l_id and ronde = l_ronde;	  
			when 'Beste tempo'                then update garmin_details set Beste_tempo = to_ds  (l_next, 'Beste tempo')                   where garmin_id = l_id and ronde = l_ronde;	  
			when 'Max. loopcadans'            then update garmin_details set max_loopcadans = to_nr (l_next, 'Max. loopcadans')             where garmin_id = l_id and ronde = l_ronde;	  
			when 'Gemiddeld bewogen tempo'    then update garmin_details set gem_bewogen_tempo = to_ds  (l_next, 'Gemiddeld bewogen tempo') where garmin_id = l_id and ronde = l_ronde;	  
	        when 'Gemiddelde snelheid'        then update garmin_details set Gemiddelde_snelheid = to_nr (l_next, 'Gemiddelde_snelheid')    where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Gem. GAP'                   then update garmin_details set Gem_GAP = l_next                                               where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Gem. HS'                    then update garmin_details set Gem_HS = to_nr (l_next, 'Gem_HS')                              where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Max. HS'                    then update garmin_details set Max_HS = to_nr (l_next, 'Max_HS')                              where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Calorieδn'                  then update garmin_details set Calorieen = to_nr (l_next, 'Calorieen')                        where garmin_id = l_id and ronde = l_ronde;	  
	        when 'Totale stijging'            then update garmin_details set Totale_stijging = to_nr (l_next, 'Totale_stijging')            where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Totale daling'              then update garmin_details set Totale_daling = to_nr (l_next, 'Totale_daling')                where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Max. snelheid'              then update garmin_details set Max_snelheid = to_nr (l_next, 'Max. snelheid')                 where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Tijd bewogen'               then update garmin_details set Tijd_bewogen = to_ds  (l_next, 'Tijd bewogen')                 where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Gemiddelde bewogen snelheid' then update garmin_details set Gem_bewogen_snelheid = to_nr (l_next, 'Gemiddelde bewogen snelheid') where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Interval'                   then update garmin_details set interval = to_nr (l_next, 'Interval')                          where garmin_id = l_id and ronde = l_ronde;		  
	        when 'Staptype'                   then update garmin_details set staptype = l_next                                              where garmin_id = l_id and ronde = l_ronde;		  
	        else
		     insert into garmin_missing_columns (table_name, column_name, pk1,  pk2, val) values ('GARMIN_DETAILS', l_column_header (i), l_id, l_ronde, l_next);   
	        end case;
		  end loop;
	    end loop;
    end if;		
  end if;
  utl_file.fclose_all;
  
  exception when others then 
    util.show_error ('Error for file: ' || f.fn, sqlerrm);
  end;
  end loop;
commit;

exception when others then 
   util.show_error ('Error in procedure load_garmin_data', sqlerrm);
end load_garmin_data;

/******************************************************************************************************************************************************************/

--
-- Move files from garmin directory to a backup directory and add time extention
--
procedure move_csv_files_to_backup
is
begin 
  for f in (select file_name, substr(file_name, instr (file_name, chr(92), 1, 3) +1) fn  from table (get_file_name ('C:\Work\garmin\ ', 'csv')))
  loop 
    begin 
      utl_file.frename ('GARMIN', f.fn, 'GARMIN_BACKUP', f.fn || '_' || to_char (sysdate, 'YYYYMonDD'), true);
	  
    exception when others then 
      util.show_error ('Error moving file: ' || f.fn, sqlerrm);
    end;
  end loop;
  
exception when others then 
   util.show_error ('Error in procedure move_csv_files_to_backup', sqlerrm);
end move_csv_files_to_backup;

/******************************************************************************************************************************************************************/

--
-- Move files from Downloads to GARMIN directory.
--
procedure move_from_downloads_to_garmin (p_nick_name in varchar2 default null)
is
begin
  for j in (select first_name from garmin_users where nick_name = nvl (p_nick_name, nick_name))
  loop
    for f in (select substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Users' || chr (92) || j.first_name || '\Downloads', 'csv')) where upper (file_name) like '%ACTIVIT%')
    loop
      begin
        utl_file.frename ('DOWNLOADS_' || upper (j.first_name), f.fn, 'GARMIN', f.fn);
      exception when others then null;
      end;
    end loop;
  end loop;

exception when others then 
   util.show_error ('Error in procedure move_from_downloads_to_garmin for nick name: ' || p_nick_name, sqlerrm);
end move_from_downloads_to_garmin;

/******************************************************************************************************************************************************************/

--
-- Routine to call several other subroutines
--
procedure load_csv_file (p_nick_name in varchar2, p_garmin_id in integer default null, p_remove in integer default 1)
is
l_first_name garmin_users.first_name%type;
begin
  select first_name into l_first_name from garmin_users where p_nick_name = p_nick_name;
  move_from_downloads_to_garmin (l_first_name);
  garmin_pkg.load_garmin_data (l_first_name, p_garmin_id);
  if p_remove = 1
  then 
    garmin_pkg.move_csv_files_to_backup;
  end if;

exception when others then 
   util.show_error ('Error in procedure load_csv_file', sqlerrm);
end load_csv_file;

/******************************************************************************************************************************************************************/

--
-- Convert interval to seconds
--
function interval_ds_to_seconds (p_interval in interval day to second) return integer
is 
begin 
  return 3600 * extract (hour from p_interval) + 60 * extract (minute from p_interval) + extract (second from p_interval);

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
begin 
  return to_dsinterval ( '00 ' || to_char (p_seconds / 3600) || ':' || to_char (mod (p_seconds, 3600) / 60) || ':' || to_char (mod (p_seconds, 60)));

exception when others then 
   util.show_error ('Error in function seconds_to_ds_interval for: ' || p_seconds, sqlerrm);
   return null;
end seconds_to_ds_interval;

/******************************************************************************************************************************************************************/

--
-- Number of seconds that have past cince the 1-st of Jan 1990
--
function date_offset_to_date (p_offset in integer) return date
is
begin 
  return to_date ('01-01-1990', 'DD-MM-YYYY') + p_offset / 3600 / 24;

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
-- The Oracle scheduler job is able to run external routines. Requirement is that the external jib scheduler service is running
-- https://developer.garmin.com/fit/download/
-- https://developer.garmin.com/fit/protocol/
--
procedure convert_fit_file_to_csv (p_file_name in varchar2)
is
l_job_name    varchar2 (100) := dbms_scheduler.generate_job_name;
begin 
   dbms_scheduler.create_job (job_name    => l_job_name,
                              job_type    => 'executable',
                              job_action  => 'C:\Work\garmin_bu\FitSDKRelease_21.115.00\FitSDKRelease_21.115.00\java\FitToCSV-data.bat',
                              number_of_arguments => 1,
                              auto_drop   => true);
   dbms_scheduler.set_job_argument_value (l_job_name, 1, p_file_name);
   dbms_output.put_line ('Job name:  ' || l_job_name);
   dbms_scheduler.run_job (l_job_name);
   dbms_scheduler.drop_job (l_job_name);
   commit;

exception when others then
   dbms_scheduler.drop_job(l_job_name);
   commit;
   util.show_error ('Error in procedure convert_fit_file_to_csv for: ' || p_file_name, sqlerrm);
end convert_fit_file_to_csv;


/******************************************************************************************************************************************************************/

--
-- Create the controlfile to be used for sqlldr
--
procedure create_sqlldr_control_file (p_file_name in varchar2)
is
l_filehandle utl_file.file_type;
l_no_cols    integer (3) := 200;
begin
  begin 
   utl_file.fremove ('GARMIN', 'load.par');
  exception when others then null;
  end;

  l_filehandle := utl_file.fopen ('GARMIN', 'load.par', 'w');
  utl_file.put_line (l_filehandle, 'options (skip 1)'); 
  utl_file.put_line (l_filehandle, 'load data');
  utl_file.put_line (l_filehandle, 'infile ''' || p_file_name || '''');
  utl_file.put_line (l_filehandle, 'badfile ''C:\Work\garmin\fit.bad''');
  utl_file.put_line (l_filehandle, 'discardfile ''C:\Work\garmin\fit.dsc''');
  utl_file.put_line (l_filehandle, 'truncate');  -- intermediate table, so truncate old contents
  utl_file.put_line (l_filehandle, 'into table garmin_load_tags');  
  utl_file.put_line (l_filehandle, 'fields terminated by "," optionally enclosed by ''"''  trailing nullcols');
  utl_file.put_line (l_filehandle, '(');
  for j in 1 .. l_no_cols
  loop
    if j != l_no_cols
	then utl_file.put_line (l_filehandle, 'COL_' || to_char(j) || ',');
	else utl_file.put_line (l_filehandle, 'COL_' || to_char(j) || ')');
	end if;
  end loop;
  utl_file.fclose (l_filehandle);

exception when others then 
   if utl_file.is_open (l_filehandle) then utl_file.fclose (l_filehandle); end if;
   util.show_error ('Error in procedure create_sqlldr_control_file for: ' || p_file_name, sqlerrm);
end create_sqlldr_control_file;   

/******************************************************************************************************************************************************************/

-- job_action  => 'C:\app\oracle\product\19.3\bin\sqlldr',
-- This routine is still open, because it refuses to run sqlldr
-- /c 
procedure load_converted_fit_file_with_sqlldr
is
l_job_name    varchar2 (100) := dbms_scheduler.generate_job_name;
begin 
   dbms_scheduler.create_job (job_name    => l_job_name,
                              job_type    => 'executable',
							  job_action  => 'C:\app\oracle\product\19.3\bin\sqlldr',                 
                              number_of_arguments => 2,
                              auto_drop   => true);						  
   dbms_scheduler.set_job_argument_value (l_job_name, 1, g_username || '/' || g_password || '@' || g_connect_string);
   dbms_scheduler.set_job_argument_value (l_job_name, 2, 'control=C:\Work\garmin\load.par');
   dbms_output.put_line ('Job name:  ' || l_job_name);
--   dbms_scheduler.set_attribute (l_job_name, 'credential_name', 'Theo');
   dbms_scheduler.run_job (l_job_name);
   commit;
   dbms_scheduler.drop_job (l_job_name);

exception when others then
   dbms_scheduler.drop_job (l_job_name);
   commit;
   util.show_error ('Error in procedure load_converted_fit_file_with_sqlldr', sqlerrm);
end load_converted_fit_file_with_sqlldr;

/******************************************************************************************************************************************************************/

--
-- Fill table garmin_fit_data
--
procedure extract_detailed_data (p_garmin_id in integer, p_person_id in number)
is
begin
  for j in (select id, attribute, value, dimension from v_fit_data)
  loop
	case j.attribute
	when 'avg_heart_rate'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, avg_heart_rate) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set avg_heart_rate = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'avg_power'
	then
	  if  to_number (j.value) > 30 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, avg_power) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set avg_power = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'max_power'
	then
	  if  to_number (j.value) > 30 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, max_power) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set max_power = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'power'
	then
	  if  to_number (j.value) > 30 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, power) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set power = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'start_position_lat'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, start_position_lat) values (p_garmin_id, j.id, p_person_id, garmin_pkg.semicircles_to_lon_lat (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set start_position_lat = garmin_pkg.semicircles_to_lon_lat (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'start_position_long'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, start_position_long) values (p_garmin_id, j.id, p_person_id, garmin_pkg.semicircles_to_lon_lat (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set start_position_long = garmin_pkg.semicircles_to_lon_lat (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'step_length'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, step_length) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set step_length = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'avg_step_length'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, avg_step_length) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set avg_step_length = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'distance'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, distance) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set distance = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'total_distance'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, total_distance) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set total_distance = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'enhanced_avg_speed'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, enhanced_avg_speed) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set enhanced_avg_speed = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'enhanced_speed'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, enhanced_speed) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set enhanced_speed = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'heart_rate'
	then
	  if  to_number (j.value) > 30 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, heart_rate) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set heart_rate = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'max_heart_rate'
	then
	  if  to_number (j.value) > 30 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, max_heart_rate) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set max_heart_rate = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'num_laps'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, num_laps) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set num_laps = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'position_lat'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, position_lat) values (p_garmin_id, j.id, p_person_id, garmin_pkg.semicircles_to_lon_lat (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set position_lat = garmin_pkg.semicircles_to_lon_lat (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'position_long'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, position_long) values (p_garmin_id, j.id, p_person_id, garmin_pkg.semicircles_to_lon_lat (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set position_long = garmin_pkg.semicircles_to_lon_lat (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'start_time'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, start_time) values (p_garmin_id, j.id, p_person_id, garmin_pkg.date_offset_to_date (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set start_time = garmin_pkg.date_offset_to_date (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'timestamp'
	then
	  if  to_number (j.value) > 100000 then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, timestamp) values (p_garmin_id, j.id, p_person_id, garmin_pkg.date_offset_to_date (to_number (j.value)));
	  exception when dup_val_on_index
	  then update garmin_fit_data set timestamp = garmin_pkg.date_offset_to_date (to_number (j.value)) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
	  end if;
--
	when 'total_calories'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, total_calories) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set total_calories = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'total_elapsed_time'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, total_elapsed_time) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set total_elapsed_time = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'total_training_effect'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, total_training_effect) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set total_training_effect = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
--
	when 'total_work'
	then
	  begin
	  insert into garmin_fit_data (garmin_id, id, person_id, total_work) values (p_garmin_id, j.id, p_person_id, to_number (j.value));
	  exception when dup_val_on_index
	  then update garmin_fit_data set total_work = to_number (j.value) where garmin_id = p_garmin_id and id = j.id and person_id = p_person_id;
	  end;
    else null;
    end case;
  end loop;
  update garmin_map_fit_file_to_id set loaded = 1 where garmin_id = p_garmin_id;
  commit;

exception when others then 
   util.show_error ('Error in procedure extract_detailed_data for user with ID: ' || p_person_id || ' and Garmin ID:  ' || p_garmin_id, sqlerrm);
end extract_detailed_data;

/******************************************************************************************************************************************************************/

--
-- Map the .fit files from the watch to a garmin_id
--
procedure map_fit_files_to_garmin_id
is 
l_found boolean;
begin 
  delete garmin_map_fit_file_to_id where garmin_id is null;
  for n in (select id, first_name from garmin_users)
  loop
    for j in (select substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Work\garmin\fit_' || n.first_name || chr(92), 'fit')))
    loop 
      l_found := false;
      for t in (select id from garmin where to_char (datum, 'YYYY-MM-DD-HH24-MI') = substr (j.fn, 1, 16) and person_id = n.id )
      loop
        l_found := true;
	    begin
          insert into garmin_map_fit_file_to_id (garmin_id, person_id, file_name, loaded) values (t.id, n.id, j.fn, 0);
	    exception when dup_val_on_index then null;
	    end;
      end loop;
      if not l_found then insert into garmin_map_fit_file_to_id (person_id, file_name, loaded) values (n.id, j.fn, 0); end if;
    end loop;
  end loop;
  commit;

exception when others then 
   util.show_error ('Error in procedure map_fit_files_to_garmin_id', sqlerrm);
end map_fit_files_to_garmin_id;

/******************************************************************************************************************************************************************/

--
-- Convert the fit files to CSV and generate the sqlldr file. Running sqlldr remains a manual task. Prioritize users by giving their nick name as parameter
--
procedure start_load_fit_data (p_nick_name in varchar2 default null)
is
l_fit_file    varchar2 (30);
l_file_name   varchar2 (100);
l_user_id     garmin_users.id%type;
l_user_name   garmin_users.nick_name%type;
begin
  map_fit_files_to_garmin_id;
  if p_nick_name is not null then select id, first_name into l_user_id, l_user_name from garmin_users where nick_name = p_nick_name; end if;

  select garmin_id, person_id, file_name into g_garmin_id, g_user_id, l_fit_file from garmin_map_fit_file_to_id
  where garmin_id = (select max (garmin_id) from garmin_map_fit_file_to_id where person_id = nvl (l_user_id, person_id) and loaded = 0);
  
  if l_user_name is null then select nick_name into l_user_name from garmin_users where id = g_user_id; end if;
  
  l_file_name := 'C:\Work\garmin\fit_' || l_user_name || chr (92) || l_fit_file;
  dbms_output.put_line ('Converting file: ' || l_file_name);
  garmin_pkg.convert_fit_file_to_csv (l_file_name);
  l_file_name := 'C:\Work\garmin\fit_' || l_user_name || chr (92) || substr (l_fit_file, 1, 19) || '.csv';
  dbms_output.put_line ('Creating controlfile for: ' || l_file_name);
  garmin_pkg.create_sqlldr_control_file  (l_file_name);
  dbms_output.put_line ('host sqlldr theo/<pwd> control=C:\Work\garmin\load.par');

exception when others then 
   util.show_error ('Error in procedure start_load_fit_data for person: ' || p_nick_name, sqlerrm);
end start_load_fit_data;

/******************************************************************************************************************************************************************/

--
-- After running sqlldr this remains a manual task
--
procedure end_load_fit_data (p_remove_csv in boolean default true)
is
begin
  garmin_pkg.extract_detailed_data (g_garmin_id, g_user_id);
  if p_remove_csv
  then 
    for u in (select first_name from garmin_users)
    loop  
      for j in (select substr(file_name, instr (file_name, chr(92), 1, 4) + 1) fn from table (get_file_name ('C:\Work\garmin\fit_' || u.first_name || chr(92), 'csv')))
      loop 
         utl_file.fremove ('FIT_' || upper (u.first_name), j.fn);
	  end loop;
    end loop;
  end if;

exception when others then 
   util.show_error ('Error in procedure end_load_fit_data for ID: ' || g_garmin_id || ' and user ID: ' || g_user_id, sqlerrm);
end end_load_fit_data;

/******************************************************************************************************************************************************************/

--
-- Returning corresponding range haertrate
--
function get_heartrate (p_person_id in integer, p_range in integer) return integer
is
l_heartrate number (3);
begin
  select case p_range when 1 then hr_low when 2 then hr_medium when 3 then hr_high else null end into l_heartrate from garmin_users where id = p_range;
  return l_heartrate;

exception when others then 
   util.show_error ('Error in function get_heartrate for userid: ' || p_person_id || ' and range: ' || p_range, sqlerrm);
end get_heartrate;

end garmin_pkg;
/


/* Demo

exec garmin_pkg.start_load_fit_data ('Dolly')
!! Run sqlldr
exec garmin_pkg.end_load_fit_data

exec garmin_pkg.load_garmin_data ('Theo')
exec garmin_pkg.move_csv_files_to_backup
exec garmin_pkg.load_csv_file ('Dolly')
exec garmin_pkg.move_from_downloads_to_garmin ('Dolly')
exec garmin_pkg. extract_detailed_data (1,'Theo')
exec garmin_pkg.create_sqlldr_control_file ('C:\Work\garmin\fit_Dolly\2023-09-22-13-46-47_fittocsv.csv')
exec garmin_pkg.load_converted_fit_file_with_sqlldr


-- First make sure the jobschedulerservice is running
exec garmin_pkg.convert_fit_file_to_csv ('C:\Work\garmin\fit_theo\2023-05-12-12-32-51.fit')

remove: 2023-05-12-12-32-51_data.csv
move 
2. The sqlldr controlfile to load the data
C:\app\oracle\product\19.3\bin\sqlldr theo/<pwd>14@db19c control=C:\Work\garmin\load.par
2. The sqlldr controlfile to load the data
C:\app\oracle\product\19.3\bin\sqlldr theo/<pwd>@db19c control=C:\Work\garmin\load.par


select * from table(get_file_name ('C:\Work\garmin\ ', 'csv'));



2. Next step is to load the csv file into an oracle table. First determine all attributes from the csv

drop table garmin_tags cascade constraints;
create table garmin_tags
( ttype varchar2 (30)
, ttag   varchar2 (100));
alter table garmin_tags add constraint garmin_tags_pk primary key (ttype, ttag);

select 'COL_' ||level || '   varchar2 (100) , ' stmt from dual connect by level <= 200;
select 'COL_' ||level || ', ' stmt from dual connect by level <= 200;

drop table garmin_load_tags;
create table garmin_load_tags
(id integer generated always as identity,
COL_1   varchar2 (100) ,
COL_2   varchar2 (100) ,
COL_3   varchar2 (100) ,
COL_4   varchar2 (100) ,
COL_5   varchar2 (100) ,
COL_6   varchar2 (100) ,
COL_7   varchar2 (100) ,
COL_8   varchar2 (100) ,
COL_9   varchar2 (100) ,
COL_10   varchar2 (100) ,
COL_11   varchar2 (100) ,
COL_12   varchar2 (100) ,
COL_13   varchar2 (100) ,
COL_14   varchar2 (100) ,
COL_15   varchar2 (100) ,
COL_16   varchar2 (100) ,
COL_17   varchar2 (100) ,
COL_18   varchar2 (100) ,
COL_19   varchar2 (100) ,
COL_20   varchar2 (100) ,
COL_21   varchar2 (100) ,
COL_22   varchar2 (100) ,
COL_23   varchar2 (100) ,
COL_24   varchar2 (100) ,
COL_25   varchar2 (100) ,
COL_26   varchar2 (100) ,
COL_27   varchar2 (100) ,
COL_28   varchar2 (100) ,
COL_29   varchar2 (100) ,
COL_30   varchar2 (100) ,
COL_31   varchar2 (100) ,
COL_32   varchar2 (100) ,
COL_33   varchar2 (100) ,
COL_34   varchar2 (100) ,
COL_35   varchar2 (100) ,
COL_36   varchar2 (100) ,
COL_37   varchar2 (100) ,
COL_38   varchar2 (100) ,
COL_39   varchar2 (100) ,
COL_40   varchar2 (100) ,
COL_41   varchar2 (100) ,
COL_42   varchar2 (100) ,
COL_43   varchar2 (100) ,
COL_44   varchar2 (100) ,
COL_45   varchar2 (100) ,
COL_46   varchar2 (100) ,
COL_47   varchar2 (100) ,
COL_48   varchar2 (100) ,
COL_49   varchar2 (100) ,
COL_50   varchar2 (100) ,
COL_51   varchar2 (100) ,
COL_52   varchar2 (100) ,
COL_53   varchar2 (100) ,
COL_54   varchar2 (100) ,
COL_55   varchar2 (100) ,
COL_56   varchar2 (100) ,
COL_57   varchar2 (100) ,
COL_58   varchar2 (100) ,
COL_59   varchar2 (100) ,
COL_60   varchar2 (100) ,
COL_61   varchar2 (100) ,
COL_62   varchar2 (100) ,
COL_63   varchar2 (100) ,
COL_64   varchar2 (100) ,
COL_65   varchar2 (100) ,
COL_66   varchar2 (100) ,
COL_67   varchar2 (100) ,
COL_68   varchar2 (100) ,
COL_69   varchar2 (100) ,
COL_70   varchar2 (100) ,
COL_71   varchar2 (100) ,
COL_72   varchar2 (100) ,
COL_73   varchar2 (100) ,
COL_74   varchar2 (100) ,
COL_75   varchar2 (100) ,
COL_76   varchar2 (100) ,
COL_77   varchar2 (100) ,
COL_78   varchar2 (100) ,
COL_79   varchar2 (100) ,
COL_80   varchar2 (100) ,
COL_81   varchar2 (100) ,
COL_82   varchar2 (100) ,
COL_83   varchar2 (100) ,
COL_84   varchar2 (100) ,
COL_85   varchar2 (100) ,
COL_86   varchar2 (100) ,
COL_87   varchar2 (100) ,
COL_88   varchar2 (100) ,
COL_89   varchar2 (100) ,
COL_90   varchar2 (100) ,
COL_91   varchar2 (100) ,
COL_92   varchar2 (100) ,
COL_93   varchar2 (100) ,
COL_94   varchar2 (100) ,
COL_95   varchar2 (100) ,
COL_96   varchar2 (100) ,
COL_97   varchar2 (100) ,
COL_98   varchar2 (100) ,
COL_99   varchar2 (100) ,
COL_100   varchar2 (100) ,
COL_101   varchar2 (100) ,
COL_102   varchar2 (100) ,
COL_103   varchar2 (100) ,
COL_104   varchar2 (100) ,
COL_105   varchar2 (100) ,
COL_106   varchar2 (100) ,
COL_107   varchar2 (100) ,
COL_108   varchar2 (100) ,
COL_109   varchar2 (100) ,
COL_110   varchar2 (100) ,
COL_111   varchar2 (100) ,
COL_112   varchar2 (100) ,
COL_113   varchar2 (100) ,
COL_114   varchar2 (100) ,
COL_115   varchar2 (100) ,
COL_116   varchar2 (100) ,
COL_117   varchar2 (100) ,
COL_118   varchar2 (100) ,
COL_119   varchar2 (100) ,
COL_120   varchar2 (100) ,
COL_121   varchar2 (100) ,
COL_122   varchar2 (100) ,
COL_123   varchar2 (100) ,
COL_124   varchar2 (100) ,
COL_125   varchar2 (100) ,
COL_126   varchar2 (100) ,
COL_127   varchar2 (100) ,
COL_128   varchar2 (100) ,
COL_129   varchar2 (100) ,
COL_130   varchar2 (100) ,
COL_131   varchar2 (100) ,
COL_132   varchar2 (100) ,
COL_133   varchar2 (100) ,
COL_134   varchar2 (100) ,
COL_135   varchar2 (100) ,
COL_136   varchar2 (100) ,
COL_137   varchar2 (100) ,
COL_138   varchar2 (100) ,
COL_139   varchar2 (100) ,
COL_140   varchar2 (100) ,
COL_141   varchar2 (100) ,
COL_142   varchar2 (100) ,
COL_143   varchar2 (100) ,
COL_144   varchar2 (100) ,
COL_145   varchar2 (100) ,
COL_146   varchar2 (100) ,
COL_147   varchar2 (100) ,
COL_148   varchar2 (100) ,
COL_149   varchar2 (100) ,
COL_150   varchar2 (100) ,
COL_151   varchar2 (100) ,
COL_152   varchar2 (100) ,
COL_153   varchar2 (100) ,
COL_154   varchar2 (100) ,
COL_155   varchar2 (100) ,
COL_156   varchar2 (100) ,
COL_157   varchar2 (100) ,
COL_158   varchar2 (100) ,
COL_159   varchar2 (100) ,
COL_160   varchar2 (100) ,
COL_161   varchar2 (100) ,
COL_162   varchar2 (100) ,
COL_163   varchar2 (100) ,
COL_164   varchar2 (100) ,
COL_165   varchar2 (100) ,
COL_166   varchar2 (100) ,
COL_167   varchar2 (100) ,
COL_168   varchar2 (100) ,
COL_169   varchar2 (100) ,
COL_170   varchar2 (100) ,
COL_171   varchar2 (100) ,
COL_172   varchar2 (100) ,
COL_173   varchar2 (100) ,
COL_174   varchar2 (100) ,
COL_175   varchar2 (100) ,
COL_176   varchar2 (100) ,
COL_177   varchar2 (100) ,
COL_178   varchar2 (100) ,
COL_179   varchar2 (100) ,
COL_180   varchar2 (100) ,
COL_181   varchar2 (100) ,
COL_182   varchar2 (100) ,
COL_183   varchar2 (100) ,
COL_184   varchar2 (100) ,
COL_185   varchar2 (100) ,
COL_186   varchar2 (100) ,
COL_187   varchar2 (100) ,
COL_188   varchar2 (100) ,
COL_189   varchar2 (100) ,
COL_190   varchar2 (100) ,
COL_191   varchar2 (100) ,
COL_192   varchar2 (100) ,
COL_193   varchar2 (100) ,
COL_194   varchar2 (100) ,
COL_195   varchar2 (100) ,
COL_196   varchar2 (100) ,
COL_197   varchar2 (100) ,
COL_198   varchar2 (100) ,
COL_199   varchar2 (100) ,
COL_200   varchar2 (100) );


2. The sqlldr controlfile to load the data
C:\app\oracle\product\19.3\bin\sqlldr theo/<pwd>@db19c control=C:\Work\garmin\load.par

load data
 infile 'C:\Work\garmin\fit.csv'
 into table load_tags  (load data
  infile 'C:\Work\garmin\fit.csv'
  badfile 'C:\Work\garmin\fit.bad'
  discardfile 'C:\Work\garmin\fit.dsc'
truncate
 into table garmin_load_tags
fields terminated by "," optionally enclosed by '"'  trailing nullcols
(COL_1,
COL_2,
COL_3,
COL_4,
COL_5,
COL_6,
COL_7,
COL_8,
COL_9,
COL_10,
COL_11,
COL_12,
COL_13,
COL_14,
COL_15,
COL_16,
COL_17,
COL_18,
COL_19,
COL_20,
COL_21,
COL_22,
COL_23,
COL_24,
COL_25,
COL_26,
COL_27,
COL_28,
COL_29,
COL_30,
COL_31,
COL_32,
COL_33,
COL_34,
COL_35,
COL_36,
COL_37,
COL_38,
COL_39,
COL_40,
COL_41,
COL_42,
COL_43,
COL_44,
COL_45,
COL_46,
COL_47,
COL_48,
COL_49,
COL_50,
COL_51,
COL_52,
COL_53,
COL_54,
COL_55,
COL_56,
COL_57,
COL_58,
COL_59,
COL_60,
COL_61,
COL_62,
COL_63,
COL_64,
COL_65,
COL_66,
COL_67,
COL_68,
COL_69,
COL_70,
COL_71,
COL_72,
COL_73,
COL_74,
COL_75,
COL_76,
COL_77,
COL_78,
COL_79,
COL_80,
COL_81,
COL_82,
COL_83,
COL_84,
COL_85,
COL_86,
COL_87,
COL_88,
COL_89,
COL_90,
COL_91,
COL_92,
COL_93,
COL_94,
COL_95,
COL_96,
COL_97,
COL_98,
COL_99,
COL_100,
COL_101,
COL_102,
COL_103,
COL_104,
COL_105,
COL_106,
COL_107,
COL_108,
COL_109,
COL_110,
COL_111,
COL_112,
COL_113,
COL_114,
COL_115,
COL_116,
COL_117,
COL_118,
COL_119,
COL_120,
COL_121,
COL_122,
COL_123,
COL_124,
COL_125,
COL_126,
COL_127,
COL_128,
COL_129,
COL_130,
COL_131,
COL_132,
COL_133,
COL_134,
COL_135,
COL_136,
COL_137,
COL_138,
COL_139,
COL_140,
COL_141,
COL_142,
COL_143,
COL_144,
COL_145,
COL_146,
COL_147,
COL_148,
COL_149,
COL_150,
COL_151,
COL_152,
COL_153,
COL_154,
COL_155,
COL_156,
COL_157,
COL_158,
COL_159,
COL_160,
COL_161,
COL_162,
COL_163,
COL_164,
COL_165,
COL_166,
COL_167,
COL_168,
COL_169,
COL_170,
COL_171,
COL_172,
COL_173,
COL_174,
COL_175,
COL_176,
COL_177,
COL_178,
COL_179,
COL_180,
COL_181,
COL_182,
COL_183,
COL_184,
COL_185,
COL_186,
COL_187,
COL_188,
COL_189,
COL_190,
COL_191,
COL_192,
COL_193,
COL_194,
COL_195,
COL_196,
COL_197,
COL_198,
COL_199,
COL_200)
)



set serveroutput on


4. Genral layout of the csv file is {attribute, value, dimension}*

create or replace view v_fit_data as
select id, col_4 attribute, col_5 value, col_6 dimension from garmin_load_tags where col_4!= 'unknown' and  col_4 is not null union all
select id, col_7, col_8, col_9 from garmin_load_tags where col_7!= 'unknown' and  col_7 is not null union all
select id, col_10, col_11, col_12 from garmin_load_tags where col_10!= 'unknown' and  col_10 is not null union all
select id, col_13, col_14, col_15 from garmin_load_tags where col_13!= 'unknown' and  col_13 is not null union all
select id, col_16, col_17, col_18 from garmin_load_tags where col_16!= 'unknown' and  col_16 is not null union all
select id, col_19, col_20, col_21 from garmin_load_tags where col_19!= 'unknown' and  col_19 is not null union all
select id, col_22, col_23, col_24 from garmin_load_tags where col_22!= 'unknown' and  col_22 is not null union all
select id, col_25, col_26, col_27 from garmin_load_tags where col_25!= 'unknown' and  col_25 is not null union all
select id, col_28, col_29, col_30 from garmin_load_tags where col_28!= 'unknown' and  col_28 is not null union all
select id, col_31, col_32, col_33 from garmin_load_tags where col_31!= 'unknown' and  col_31 is not null union all
select id, col_34, col_35, col_36 from garmin_load_tags where col_34!= 'unknown' and  col_34 is not null union all
select id, col_37, col_38, col_39 from garmin_load_tags where col_37!= 'unknown' and  col_37 is not null union all
select id, col_40, col_41, col_42 from garmin_load_tags where col_40!= 'unknown' and  col_40 is not null union all
select id, col_43, col_44, col_45 from garmin_load_tags where col_43!= 'unknown' and  col_43 is not null union all
select id, col_46, col_47, col_48 from garmin_load_tags where col_46!= 'unknown' and  col_46 is not null union all
select id, col_49, col_50, col_51 from garmin_load_tags where col_49!= 'unknown' and  col_49 is not null union all
select id, col_52, col_53, col_54 from garmin_load_tags where col_52!= 'unknown' and  col_52 is not null union all
select id, col_55, col_56, col_57 from garmin_load_tags where col_55!= 'unknown' and  col_55 is not null union all
select id, col_58, col_59, col_60 from garmin_load_tags where col_58!= 'unknown' and  col_58 is not null union all
select id, col_61, col_62, col_63 from garmin_load_tags where col_61!= 'unknown' and  col_61 is not null union all
select id, col_64, col_65, col_66 from garmin_load_tags where col_64!= 'unknown' and  col_64 is not null union all
select id, col_67, col_68, col_69 from garmin_load_tags where col_67!= 'unknown' and  col_67 is not null union all
select id, col_70, col_71, col_72 from garmin_load_tags where col_70!= 'unknown' and  col_70 is not null union all
select id, col_73, col_74, col_75 from garmin_load_tags where col_73!= 'unknown' and  col_73 is not null union all
select id, col_76, col_77, col_78 from garmin_load_tags where col_76!= 'unknown' and  col_76 is not null union all
select id, col_79, col_80, col_81 from garmin_load_tags where col_79!= 'unknown' and  col_79 is not null union all
select id, col_82, col_83, col_84 from garmin_load_tags where col_82!= 'unknown' and  col_82 is not null union all
select id, col_85, col_86, col_87 from garmin_load_tags where col_85!= 'unknown' and  col_85 is not null union all
select id, col_88, col_89, col_90 from garmin_load_tags where col_88!= 'unknown' and  col_88 is not null union all
select id, col_91, col_92, col_93 from garmin_load_tags where col_91!= 'unknown' and  col_91 is not null union all
select id, col_94, col_95, col_96 from garmin_load_tags where col_94!= 'unknown' and  col_94 is not null union all
select id, col_97, col_98, col_99 from garmin_load_tags where col_97!= 'unknown' and  col_97 is not null union all
select id, col_100, col_101, col_102 from garmin_load_tags where col_100!= 'unknown' and  col_100 is not null union all
select id, col_103, col_104, col_105 from garmin_load_tags where col_103!= 'unknown' and  col_103 is not null union all
select id, col_106, col_107, col_108 from garmin_load_tags where col_106!= 'unknown' and  col_106 is not null union all
select id, col_109, col_110, col_111 from garmin_load_tags where col_109!= 'unknown' and  col_109 is not null union all
select id, col_112, col_113, col_114 from garmin_load_tags where col_112!= 'unknown' and  col_112 is not null union all
select id, col_115, col_116, col_117 from garmin_load_tags where col_115!= 'unknown' and  col_115 is not null union all
select id, col_118, col_119, col_120 from garmin_load_tags where col_118!= 'unknown' and  col_118 is not null union all
select id, col_121, col_122, col_123 from garmin_load_tags where col_121!= 'unknown' and  col_121 is not null union all
select id, col_124, col_125, col_126 from garmin_load_tags where col_124!= 'unknown' and  col_124 is not null union all
select id, col_127, col_128, col_129 from garmin_load_tags where col_127!= 'unknown' and  col_127 is not null union all
select id, col_130, col_131, col_132 from garmin_load_tags where col_130!= 'unknown' and  col_130 is not null union all
select id, col_133, col_134, col_135 from garmin_load_tags where col_133!= 'unknown' and  col_133 is not null union all
select id, col_136, col_137, col_138 from garmin_load_tags where col_136!= 'unknown' and  col_136 is not null union all
select id, col_139, col_140, col_141 from garmin_load_tags where col_139!= 'unknown' and  col_139 is not null union all
select id, col_142, col_143, col_144 from garmin_load_tags where col_142!= 'unknown' and  col_142 is not null union all
select id, col_145, col_146, col_147 from garmin_load_tags where col_145!= 'unknown' and  col_145 is not null union all
select id, col_148, col_149, col_150 from garmin_load_tags where col_148!= 'unknown' and  col_148 is not null union all
select id, col_151, col_152, col_153 from garmin_load_tags where col_151!= 'unknown' and  col_151 is not null union all
select id, col_154, col_155, col_156 from garmin_load_tags where col_154!= 'unknown' and  col_154 is not null union all
select id, col_157, col_158, col_159 from garmin_load_tags where col_157!= 'unknown' and  col_157 is not null union all
select id, col_160, col_161, col_162 from garmin_load_tags where col_160!= 'unknown' and  col_160 is not null union all
select id, col_163, col_164, col_165 from garmin_load_tags where col_163!= 'unknown' and  col_163 is not null union all
select id, col_166, col_167, col_168 from garmin_load_tags where col_166!= 'unknown' and  col_166 is not null union all
select id, col_169, col_170, col_171 from garmin_load_tags where col_169!= 'unknown' and  col_169 is not null union all
select id, col_172, col_173, col_174 from garmin_load_tags where col_172!= 'unknown' and  col_172 is not null union all
select id, col_175, col_176, col_177 from garmin_load_tags where col_175!= 'unknown' and  col_175 is not null union all
select id, col_178, col_179, col_180 from garmin_load_tags where col_178!= 'unknown' and  col_178 is not null union all
select id, col_181, col_182, col_183 from garmin_load_tags where col_181!= 'unknown' and  col_181 is not null union all
select id, col_184, col_185, col_186 from garmin_load_tags where col_184!= 'unknown' and  col_184 is not null union all
select id, col_187, col_188, col_189 from garmin_load_tags where col_187!= 'unknown' and  col_187 is not null union all
select id, col_190, col_191, col_192 from garmin_load_tags where col_190!= 'unknown' and  col_190 is not null union all
select id, col_193, col_194, col_195 from garmin_load_tags where col_193!= 'unknown' and  col_193 is not null union all
select id, col_196, col_197, col_198 from garmin_load_tags where col_196!= 'unknown' and  col_196 is not null;


select col_8 "enhanced_speed m/s" from garmin_load_tags
where col_1 = 'Data' and col_3 = 'gps_metadata';


select col_5 from garmin_load_tags where col_3 = 'sport'   and col_1 ='Data';


-- select * from v_fit_data where attribute = 'timestamp' order by id; The following attributes are important:
avg_heart_rate, avg_power, avg_step_length, cadence, distance, end_position_lat, end_position_long, enhanced_avg_speed,
 enhanced_max_speed, enhanced_speed,heart_rate,max_heart_rate, position_lat, position_long,power,step_length, 
timestamp



ToDO:
select * from garmin_fit_data where 
 (total_elapsed_time is not null  or  total_training_effect is not null)
and garmin_id = 1262 and avg_heart_rate is not null
order by garmin_id;



-- Stepwise measurements
select round(gf.distance, -2)/ .5 distance, max(gf.enhanced_speed) enhanced_speed, round(avg(heart_rate)) heart_rate,
avg(gf.position_lat) position_lat, avg(gf.position_long) position_long
from 
(select 0.5 * gfi.distance distance, gfi.enhanced_speed, gfi.heart_rate, gfi.position_lat, gfi.position_long from
garmin_fit_data gfi where gfi.garmin_id = 1 and gfi.position_lat is not null and gfi.timestamp is not null
and gfi.person_id = gfi.person_id) gf
group by round(gf.distance, -2)
 having round(avg(heart_rate)) < 130
order by 1;


 dbms_scheduler.set_attribute('MODPY_JOB','credential_name','Theo');

begin
    dbms_scheduler.create_credential(
    CREDENTIAL_NAME => 'Theo',
    USERNAME => 'Theo',
    PASSWORD => 'V3et',
    WINDOWS_DOMAIN  => 'localdomain');
end;
/

*/

