DOC

  Author   :  Theo Stienissen
  Date     :  2023
  Purpose  :  Data from Garmin watch
  Status   :  Test!!
  Contact  :  theo.stienissen@gmail.com
  @C:\Users\Theo\OneDrive\Theo\Project\Maths\RSA\rsa.sql

Garmin data export: https://www.garmin.com/en-GB/account/datamanagement/exportdata/


#

drop table garmin_details;
drop table garmin cascade constraints;
drop sequence garmin_seq;
create table garmin (
id                         integer,
persoon                    varchar2 (20),
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
create unique index garmin_uk1 on garmin (persoon, Activiteittype, Datum);
alter table garmin add constraint garmin_ck1 check (persoon in ('Dolly', 'Theo', 'Celeste'));
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

set serveroutput on size unlimited

create or replace package garmin_pkg
is
function ds_to_varchar2 (p_ds in interval day to second) return varchar2;

function details_exist (p_garmin_id in number) return number;

function to_nr (p_string in varchar2, p_col in varchar2 default null) return number;

function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second;

function to_dt (p_string in varchar2, p_col in varchar2 default null) return date;

procedure load_garmin_data (p_user in varchar2, p_garmin_id in integer default null);

procedure move_csv_files_to_backup;

procedure move_from_downloads_to_garmin (p_user in varchar2 default 'Theo');

procedure load_csv_file (p_user in varchar2, p_garmin_id in integer default null, p_remove in integer default 1);

function interval_ds_to_seconds (p_interval in interval day to second) return integer;

function seconds_to_ds_interval (p_seconds in integer) return interval day to second;
end garmin_pkg;
/


create or replace package body garmin_pkg
is

--
-- Convert "interval day to second" to a string
--
function ds_to_varchar2 (p_ds in interval day to second) return varchar2
is 
begin 
  return lpad(extract (hour from p_ds), 2, '0') || ':' || lpad(extract (minute from p_ds), 2, '0')  || ':' || lpad(extract (second from p_ds), 2, '0');

exception when others then
  util.show_error ('Error in function ds_to_varchar2', sqlerrm);
  return null;
end ds_to_varchar2;

/******************************************************************************************************************************************************************/

--
-- Check if exercise details are available
--
function details_exist (p_garmin_id in number) return number
is
l_count integer (4);
begin 
   select count (*) into l_count from garmin_details where garmin_id = p_garmin_id;
   return l_count;
   
exception when others then
  util.show_error ('Error in function details_exist', sqlerrm);
  return null;
end details_exist;

/******************************************************************************************************************************************************************/

--
-- Convert the different number (or tempo) formats
-
function to_nr (p_string in varchar2, p_col in varchar2 default null) return number
is 
l_nr    number (20,4);
begin
--  dbms_output.put_line ('to_nr: ' || p_col || '   ' || p_string);
  if p_string = '--' then return null;
  elsif p_string like '%:%' -- tempo 
  then l_nr := round(3600 / (60 * substr (p_string, 1, instr (p_string, ':') - 1) + substr (p_string, instr (p_string, ':') + 1)), 2);  
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
  elsif  instr (p_string, ':') = 0       then l_ds :=  to_dsinterval ( '00 00:00:' || replace (p_string, ',', '.'));
  elsif instr (p_string, ':', 1, 2) = 0  then l_ds :=  to_dsinterval ( '00 00:'    || replace (p_string, ',', '.'));
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

procedure load_garmin_data (p_user in varchar2, p_garmin_id in integer default null)
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
  for f in (select substr(file_name, instr (file_name, chr(92), 1, 3) +1) fn from table (get_file_name ('C:\Work\garmin' || chr(92), 'csv'))
                    where upper(file_name) like '%ACTIVIT%' order by file_name)
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
		insert into garmin (id, persoon, Activiteittype, Datum) values (l_id, p_user, l_activiteit, l_datum);
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
		
		l_next    := next_item; -- [Overzicht] Laatste regel
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
			when 'Gem. GCT balans'            then update garmin_details set gem_gct_balans = l_next          where garmin_id = l_id and ronde = l_ronde;	  
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
-- Move files to a different directory and add ad time extention
--
procedure move_csv_files_to_backup
is
begin 
  for f in (select file_name, substr(file_name, instr (file_name, chr(92), 1, 3) +1) fn  from table (get_file_name ('C:\Work\garmin\ ', 'csv')) order by file_name)
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

-- The user can have 3 values: Dolly, Theo or Celeste.
-- There is no other way to get this working for all 3 of them.
procedure move_from_downloads_to_garmin (p_user in varchar2 default 'Theo')
is
l_found boolean := false;
begin
--  if p_user = 'Theo'
--  then 
    for f in (select substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Users\Theo\Downloads', 'csv')) where upper (file_name) like '%ACTIVIT%')
    loop
      begin
        utl_file.frename ('DOWNLOADS', f.fn, 'GARMIN', f.fn);
      exception when others then null;
      end;
	  l_found := true;
    end loop;
--  elsif p_user = 'Dolly'
--  then 
    for f in (select substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn from table (get_file_name ('C:\Users\Dolly\Downloads', 'csv')) where upper (file_name) like '%ACTIVIT%')
    loop
      begin
        utl_file.frename ('DOWNLOADS_DOLLY', f.fn, 'GARMIN', f.fn);
      exception when others then null;
      end;
	  l_found := true;
    end loop;
--  end if;

exception when others then 
   util.show_error ('Error in procedure move_from_downloads_to_garmin', sqlerrm);
end move_from_downloads_to_garmin;

/******************************************************************************************************************************************************************/

procedure load_csv_file (p_user in varchar2, p_garmin_id in integer default null, p_remove in integer default 1)
is
begin
  move_from_downloads_to_garmin (p_user);
  garmin_pkg.load_garmin_data (p_user, p_garmin_id);
  if p_remove = 1
  then 
    garmin_pkg.move_csv_files_to_backup;
  end if;

exception when others then 
   util.show_error ('Error in procedure load_csv_file', sqlerrm);
end load_csv_file;

/******************************************************************************************************************************************************************/

function interval_ds_to_seconds (p_interval in interval day to second) return integer
is 
begin 
  return 3600 * extract (hour from p_interval) + 60 * extract (minute from p_interval) + extract (second from p_interval);

exception when others then 
   util.show_error ('Error in function interval_ds_to_seconds', sqlerrm);
end interval_ds_to_seconds;

/******************************************************************************************************************************************************************/

function seconds_to_ds_interval (p_seconds in integer) return interval day to second
is 
begin 
  return to_dsinterval ( '00 ' || to_char (p_seconds / 3600) || ':' || to_char (mod (p_seconds, 3600) / 60) || ':' || to_char (mod (p_seconds, 60)));

exception when others then 
   util.show_error ('Error in function seconds_to_ds_interval for: ' || p_seconds, sqlerrm);
end seconds_to_ds_interval;
end garmin_pkg;
/


exec garmin_pkg.load_garmin_data ('Theo')
exec garmin_pkg.move_csv_files_to_backup
exec garmin_pkg.load_csv_file ('Dolly')
exec garmin_pkg.move_from_downloads_to_garmin ('Theo')


select * from table(get_file_name ('C:\Work\garmin\', 'csv'));



/



begin
utl_file.frename ('GARMIN', 'activity_11794830109.csv', 'GARMIN_BACKUP', 'activity_11794830109.csv' || '_' || to_char (sysdate, 'YYYYMonDD'));
end;
/

