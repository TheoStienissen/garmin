doc

Swap rows so that all rows with zero entries are on the bottom of the matrix.
Swap rows so that the row with the largest left-most digit is on the top of the matrix.
Multiply the top row by a scalar that converts the top row’s leading entry into 
 (If the leading entry of the top row is 
, then multiply it by 
 
 to get 
 ).
Add or subtract multiples of the top row to the other rows so that the entry’s in the column of the top row’s leading entry are all zeroes.
Perform Steps 
 for the next leftmost non-zero entry until all the leading entries of each row are 
.
Swap the rows so that the leading entry of each nonzero row is to the right of the leading entry of the row directly above it

-- Additional package to ease Gauss Jordan elimination
		  
			  end if;
			  
              l_matrix := remove_zeros_rows (remove_zeros_columns (l_matrix));

              -- Step 2. Remove non zero entries above the diagonal.
              l_counter := l_counter - 1;
              while l_counter >= 1
              loop 
                1. Multiply this last row, so the first non empty column is a 1. Already done in previous step 3.
                2. For all entries above this column subtract as many times this last column, so a zero remains.
                l_counter := l_counter - 1;
              end loop;
                            Remove columns with all zero-s. (There should not be any) 

#



set serveroutput on size unlimited
declare
l_vector  types_pkg.vector_Q_ty;
l_matrix  types_pkg.matrix_Q_ty;
begin 
 l_vector := matrix_Q_pkg.to_vector (fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(1), fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(1));
 l_matrix := matrix_Q_pkg.add_row (l_matrix, 1, l_vector);
 l_vector := matrix_Q_pkg.to_vector (fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(1), fractions_pkg.to_fraction(1));
 l_matrix := matrix_Q_pkg.add_row (l_matrix, 2, l_vector); 
 l_vector := matrix_Q_pkg.to_vector (fractions_pkg.to_fraction(-1), fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(2), fractions_pkg.to_fraction(1));
 l_matrix := matrix_Q_pkg.add_row (l_matrix, 3, l_vector); 
 l_vector := matrix_Q_pkg.to_vector (fractions_pkg.to_fraction(-1), fractions_pkg.to_fraction(1), fractions_pkg.to_fraction(0), fractions_pkg.to_fraction(0));
 l_matrix := matrix_Q_pkg.add_row (l_matrix, 4, l_vector);
 matrix_Q_pkg.print_matrix (l_matrix);
 l_matrix := matrix_Q_pkg.gauss_jordan_elimination (l_matrix);
end;
/

--
--, 2=>types_pkg.vector_Q_ty(fr(5),fr(6),fr(7),fr(8)),
--3=>types_pkg.vector_Q_ty(fr(2),fr(3),fr(4),fr(5)),4=>types_pkg.vector_Q_ty(fr(3),fr(4),fr(6),fr(9)));
select rownum id, b,c,d,e,f,g,h,i, -a from table (chemistry_pkg.f_chemistry_matrix) where reaction_id = 2 order by reaction_id, symbol;

declare
l_vector  types_pkg.vector_Q_ty;
l_matrix  types_pkg.matrix_Q_ty;
l_count   integer := 1;
l_lcm     integer := 1;
begin 
for j in (select  b,c,d,e,f,g,h,i, j, -a a from table (chemistry_pkg.f_chemistry_matrix) where reaction_id = 2 order by symbol)
loop
   l_vector := matrix_Q_pkg.to_vector (fractions_pkg.to_fraction(j.b), fractions_pkg.to_fraction(j.c), fractions_pkg.to_fraction(j.d), fractions_pkg.to_fraction(j.e),
   fractions_pkg.to_fraction(j.f), fractions_pkg.to_fraction(j.g), fractions_pkg.to_fraction(j.h), fractions_pkg.to_fraction(j.i), fractions_pkg.to_fraction(j.j), fractions_pkg.to_fraction(j.a));
   l_matrix := matrix_Q_pkg.add_row (l_matrix, l_count, l_vector);
   l_count  := l_count + 1;
end loop;
  matrix_Q_pkg.print_matrix (l_matrix);
  l_matrix := matrix_Q_pkg.gauss_jordan_elimination (l_matrix);
 l_count := l_matrix (1).count;
for j in 1.. l_matrix.count 
loop 
  l_lcm := maths.lcm (l_lcm, l_matrix (j)(l_count).denominator);
end loop;
  matrix_Q_pkg.print_matrix (l_matrix);
  dbms_output.put_line ('LCM : ' || l_lcm);
for j in 1.. l_matrix.count 
loop 
  fractions_pkg.print (fractions_pkg.multiply (l_matrix (j)(l_count), fractions_pkg.to_fraction(l_lcm)));
end loop;
end;
/