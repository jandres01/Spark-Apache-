--Answers on SQL Questions by Robbie Andres

-- IC
1. show tables;
2. select * from users;
3. select count(*) from lambda_assoc;
4. select count(correct_option) from multiple_choice_questions; --(from Dr. Lewis's perspective)
   select count(selection) from mc_answers; --(from student postings)
5. select count(*) from quiz_course_close_assoc AS q INNER JOIN courses AS c ON c.code = "CSCI1320" AND c.semester = "F15" AND c.section = 6 WHERE q.quizid = c.courseid;

--OC
1. -- Answer is courseid = 12 with 26 quizzes
select courseid,count(quizid) as total from quiz_course_close_assoc group by courseid order by total DESC Limit 1;
+----------+-------+
| courseid | total |
+----------+-------+
|       12 |    26 |
+----------+-------+
2. -- Answer courseid 12 has 68 questions on quizzes
select courseid, count(lambda_question_id)+count(mc_question_id)+count(expr_question_id)+count(func_question_id) AS total from quiz_course_close_assoc AS q
          LEFT JOIN expression_assoc AS e ON q.quizid=e.quizid LEFT JOIN multiple_choice_assoc AS m ON q.quizid = m.quizid LEFT JOIN lambda_assoc AS l
          ON q.quizid = l.quizid LEFT JOIN function_assoc AS f ON q.quizid=f.quizid GROUP BY q.courseid order by total DESC limit 1;
          +----------+-------+
          | courseid | total |
          +----------+-------+
          |       12 |    68 |
          +----------+-------+
   --different way of joining
   --select courseid, (count(mc_question_id)) from quiz_course_close_assoc join multiple_choice_assoc using (quizid) group by courseid;

   --number of lambda questions for each courseID
   --select courseid, count(lambda_question_id) from lambda_assoc AS l INNER JOIN quiz_course_close_assoc AS q ON q.quizid = l.quizid GROUP BY q.courseid;

   -- number of mc questions for each courseID
   --select courseid, count(mc_question_id) from multiple_choice_assoc AS m INNER JOIN quiz_course_close_assoc AS q ON q.quizid = m.quizid GROUP BY q.courseid;

3. -- 0.1516 students who submit an answer get it correct 
SELECT count(DISTINCT userid) answers FROM code_answers WHERE question_type='1';
+---------+
| answers |
+---------+
|     162 |
+---------+

4. -- 9 courses have given questions with lambda type
select count(c.courseid) from courses as c JOIN quiz_course_close_assoc as q ON c.courseid = q.courseid JOIN lambda_assoc as l ON q.quizid=l.quizid;
+-------------------+
| count(c.courseid) |
+-------------------+
|                 9 |
+-------------------+

5. -- userid 39 has the most correct answers with 26
select userid,SUM(CASE WHEN correct =True  THEN 1 ELSE 0 END) as total from code_answers GROUP BY userid order by total DESC limit 1;
+--------+-------+
| userid | total |
+--------+-------+
|     39 |    26 |
+--------+-------+

6. select spec_type, count(spec_type) as total from variable_specifications GROUP BY spec_type order by total DESC limit 3;
+-----------+-------+
| spec_type | total |
+-----------+-------+
|         0 |    13 |
|         2 |     6 |
|         3 |     5 |
+-----------+-------+

7. SELECT (select count(distinct quizid) as c FROM code_answers) / (select count(distinct quizid) as q FROM quizzes);
+------------------------------------------------------------------------------------------------------------+
| (select count(distinct quizid) as c FROM code_answers) / (select count(distinct quizid) as q FROM quizzes) |
+------------------------------------------------------------------------------------------------------------+
|                                                                                                     0.3818 |
+------------------------------------------------------------------------------------------------------------+

-- thought you wanted percentage of coding questions out of all questions :(
--select  count(distinct c.question_id)/(count(distinct lambda_question_id)+count(distinct mc_question_id)+count(distinct expr_question_id)+ count(distinct func_question_id)) from quiz_course_close_assoc AS q
--   LEFT JOIN expression_assoc AS e ON q.quizid=e.quizid LEFT JOIN multiple_choice_assoc AS m ON q.quizid = m.quizid LEFT JOIN lambda_assoc AS l
--   ON q.quizid = l.quizid LEFT JOIN function_assoc AS f ON q.quizid=f.quizid LEFT JOIN code_answers AS c ON q.quizid=c.quizid;

8. select AVG(mCount) FROM (select q.quizid, count(m.mc_question_id) AS mCount from quizzes AS q LEFT JOIN multiple_choice_assoc AS m ON q.quizid=m.quizid GROUP BY q.quizid) a;
+-------------+
| AVG(mCount) |
+-------------+
|      2.0182 |
+-------------+

-- To Get number of MC questions each quizid
--select q.quizid, count(m.mc_question_id) from quizzes AS q LEFT JOIN multiple_choice_assoc AS m ON q.quizid=m.quizid GROUP BY q.quizid;

9. select AVG(cCount) FROM (select q.quizid, c.question_type,count(distinct c.question_id) AS cCount from quizzes AS q LEFT JOIN code_answers AS c ON q.quizid=c.quizid GROUP BY q.quizid, c.question_type) a ;
+-------------+
| AVG(cCount) |
+-------------+
|      0.6885 |
+-------------+

/*select AVG(total) FROM(
select  (count(distinct lambda_question_id)+count(distinct expr_question_id)+ count(distinct func_question_id)) AS total from quiz_course_close_assoc AS q
          LEFT JOIN expression_assoc AS e ON q.quizid=e.quizid LEFT JOIN multiple_choice_assoc AS m ON q.quizid = m.quizid LEFT JOIN lambda_assoc AS l
          ON q.quizid = l.quizid LEFT JOIN function_assoc AS f ON q.quizid=f.quizid LEFT JOIN code_answers AS c ON q.quizid=c.quizid GROUP BY q.quizid
) a; */

10. select a.userid,MAX(attempts) FROM (select userid, quizid, count(answer) AS attempts FROM code_answers AS c GROUP BY quizid,userid ORDER BY userid) a GROUP BY userid;

userid | MAX(attempts) |
+--------+---------------+
|      1 |            66 |
|      2 |             4 |
|      7 |            25 |
|      8 |            11 |
|      9 |            42 |
|     10 |             6 |
|     11 |            21 |
|     12 |            36 |
|     13 |            39 |
|     14 |            19 |
|     15 |            36 |
|     16 |             5 |
|     17 |            31 |
|     18 |            17 |
|     22 |            15 |
|     23 |            10 |
|     24 |            36 |
|     25 |             8 |
|     26 |            45 |
|     27 |            85 |
|     28 |            16 |
|     29 |            25 |
|     30 |            28 |
|     31 |             4 |
|     32 |            10 |
|     33 |            23 |
|     34 |            19 |
|     35 |            17 |
|     36 |            82 |
|     37 |            41 |
|     38 |            34 |
|     39 |            46 |
|     40 |            25 |
|     41 |            31 |
|     42 |            81 |
|     43 |            20 |
|     44 |             6 |
|     45 |            35 |
|     46 |             9 |
|     47 |            32 |
|     48 |            20 |
|     49 |            12 |
|     50 |             3 |
|     51 |            58 |
|     52 |            38 |
|     53 |             2 |
|     54 |            29 |
|     55 |            16 |
|     56 |            44 |
|     57 |            34 |
|     58 |           211 |
|     59 |             8 |
|     60 |             3 |
|     61 |            47 |
|     62 |            28 |
|     63 |            56 |
|     64 |            21 |
|     65 |            45 |
|     66 |            27 |
|     67 |            25 |
|     68 |            13 |
|     69 |             4 |
|     70 |            14 |
|     71 |             3 |
|     72 |            29 |
|     73 |             4 |
|     74 |            10 |
|     75 |            17 |
|     77 |            14 |
|     78 |            10 |
|     79 |            66 |
|     81 |             1 |
|     82 |            14 |
|     83 |            10 |
|     84 |             1 |
|     85 |            10 |
|     86 |             7 |
|     88 |             1 |
|     89 |             2 |
|     91 |            10 |
|     92 |             1 |
|     93 |            13 |
|     95 |             4 |
|     96 |            10 |
|     97 |             3 |
|     98 |             2 |
|     99 |             1 |
|    100 |             1 |
|    101 |             1 |
|    102 |             4 |
|    103 |             2 |
|    105 |             3 |
|    106 |             2 |
|    107 |             6 |
|    108 |             6 |
|    109 |             2 |
|    110 |             6 |
|    111 |             8 |
|    113 |             2 |
|    115 |             6 |
|    116 |            40 |
|    118 |            50 |
|    119 |           109 |
|    120 |             2 |
|    121 |             6 |
|    123 |            70 |
|    124 |           150 |
|    125 |            97 |
|    126 |             8 |
|    127 |            11 |
|    128 |            27 |
|    129 |            21 |
|    132 |            14 |
|    133 |             9 |
|    134 |            47 |
|    136 |            24 |
|    137 |             7 |
|    138 |            13 |
|    139 |             9 |
|    140 |             7 |
|    142 |            10 |
|    143 |             5 |
|    144 |             2 |
|    145 |             8 |
|    146 |             3 |
|    147 |             4 |
|    148 |            26 |
|    149 |             2 |
|    150 |             2 |
|    151 |            27 |
|    153 |            16 |
|    154 |            34 |
|    155 |            11 |
|    156 |            57 |
|    157 |            15 |
|    158 |            10 |
|    160 |            24 |
|    161 |            12 |
|    162 |            16 |
|    163 |            24 |
|    164 |             4 |
|    165 |            23 |
|    166 |             3 |
|    167 |             5 |
|    168 |            12 |
|    169 |            14 |
|    190 |             7 |
|    191 |             2 |
|    192 |             7 |
|    193 |            23 |
|    194 |             3 |
|    196 |             5 |
|    197 |             2 |
|    198 |            23 |
|    199 |             4 |
|    200 |             3 |
|    201 |            12 |
|    202 |            11 |
|    203 |             6 |
|    204 |            34 |
|    205 |             8 |
|    206 |             2 |
|    207 |             9 |
|    208 |            16 |
|    209 |             4 |
|    210 |             1 |
|    211 |             2 |
|    212 |             1 |
|    214 |             1 |
|    215 |             1 |
|    216 |             2 |
|    217 |             1 |
|    218 |             1 |
|    219 |             1 |
|    220 |             1 |
|    221 |             3 |
|    222 |            12 |
+--------+---------------+
