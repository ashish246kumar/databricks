40
You have a table of in-app purchases by user. Users that make their first in-app purchase are placed in a marketing
campaign where they see call-to-actions for more in-app purchases.
Find the number of users that made additional in-app purchases due to the success of the marketing campaign.

The marketing campaign doesn't start until one day after the initial in-app purchase so users that only made one or
multiple purchases on the first day do not count, nor do we count users that over time purchase is the same product as 
purchased on the first day.
<br>
*(A user counts only if:
✔ Condition 1:
They made a first purchase (everyone does).
✔ Condition 2:
They made another purchase after 1 day (campaign active).
✔ Condition 3:
The product purchased later is NOT the same as the product they bought on Day 1.
❌ Do NOT count users who:
Only purchased on the same day as first purchase.
Purchased multiple times on the first day.
Purchased only the same product again later.
)*
<br>
CREATE TABLE [marketing_campaign](
 [user_id] [int] NULL,
 [created_at] [date] NULL,
 [product_id] [int] NULL,
 [quantity] [int] NULL

 
insert into marketing_campaign values (51,'2019-02-21',120,2,99),
(51,'2019-03-13',108,4,120),
(52,'2019-02-23',117,2,999),
(52,'2019-03-18',112,5,200),
(53,'2019-02-24',120,4,99),

insert into marketing_campaign values (31,'2019-01-30',104,3,154),
(32,'2019-01-31',117,1,999),
(33,'2019-01-31',117,2,999),
(34,'2019-01-31',110,3,299),
(35,'2019-02-03',117,2,999),

*Right answer
*[
WITH first_purchase AS (
    SELECT
        user_id,
        created_at AS first_date,
        product_id AS first_product,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS rnk
    FROM marketing_campaign
),
user_first AS (
    SELECT user_id, first_date, first_product
    FROM first_purchase
    WHERE rnk = 1
),
after_first_day AS (
    SELECT m.user_id, m.created_at, m.product_id
    FROM marketing_campaign m
    JOIN user_first f
      ON m.user_id = f.user_id
    WHERE m.created_at > DATEADD(day, 1, f.first_date)     -- must be after 1 day
      AND m.product_id <> f.first_product                  -- must be different product
)
SELECT COUNT(DISTINCT user_id) AS successful_users
FROM after_first_day;
]*

<img width="262" height="493" alt="40" src="https://github.com/user-attachments/assets/92ccdfa0-f893-4f83-a709-3b9be9386d82" />

__________________________________________________________________
41

Write a SQL Query to find all couples of trade for same stock that happened in the range of 10 seconds
and having price difference by more than 10%.
Output result should also list the percentage of price difference between the 2 trade.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
Create Table Trade_tbl(
TRADE_ID varchar(20),
Trade_Timestamp time,
Trade_Stock varchar(20),
Quantity int,
Price Float
);
Insert into Trade_tbl Values('TRADE1','10:01:05','ITJunction4All',100,20)
Insert into Trade_tbl Values('TRADE2','10:01:06','ITJunction4All',20,15)
Insert into Trade_tbl Values('TRADE3','10:01:08','ITJunction4All',150,30)
Insert into Trade_tbl Values('TRADE4','10:01:09','ITJunction4All',300,32)
Insert into Trade_tbl Values('TRADE5','10:10:00','ITJunction4All',-100,19)
Insert into Trade_tbl Values('TRADE6','10:10:01','ITJunction4All',-300,19);

<img width="374" height="511" alt="41" src="https://github.com/user-attachments/assets/060500de-7381-49de-8abd-6424f0c50207" />

_______________________________________________________________________________________

42

Problem statement : we have a table which stores data of multiple sections. every section has 3 numbers
we have to find top 4 numbers from any 2 sections(2 numbers each) whose addition should be maximum
so in this case we will choose section b where we have 19(10+9) then we need to choose either C or D
because both has sum of 18 but in D we have 10 which is big from 9 so we will give priority to D.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table section_data
(
section varchar(5),
number integer
)
insert into section_data
values ('A',5),('A',7),('A',10) ,('B',7),('B',9),('B',10) ,('C',9),('C',7),('C',9) ,('D',10),('D',3),('D',8);

<img width="318" height="495" alt="42" src="https://github.com/user-attachments/assets/d98e219b-2da7-4830-ad74-b4f2d4e2ba73" />

___________________________________
43
Problem statement: We need to obtain a list of departments with an average salary lower than the overall average salary of the company.
However, when calculating the company's average salary, you must exclude the salaries of the department you are comparing it with. For instance,
when comparing the average salary of HR department with the company's average, the HR department's salaries shouldn't be taken into consideration 
for the calculations of company average salary. Essentially, the company's
average salary will be dynamic for each department.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table emp(
emp_id int,
emp_name varchar(20),
department_id int,
salary int,
manager_id int,
emp_age int);

insert into emp
values
(1, 'Ankit', 100,10000, 4, 39);
insert into emp
values (2, 'Mohit', 100, 15000, 5, 48);
insert into emp
values (3, 'Vikas', 100, 10000,4,37);
insert into emp
values (4, 'Rohit', 100, 5000, 2, 16);
insert into emp
values (5, 'Mudit', 200, 12000, 6,55);
insert into emp
values (6, 'Agam', 200, 12000,2, 14);
insert into emp
values (7, 'Sanjay', 200, 9000, 2,13);
insert into emp
values (8, 'Ashish', 200,5000,2,12);
insert into emp
values (9, 'Mukesh',300,6000,6,51);
insert into emp
values (10, 'Rakesh',300,7000,6,50);

<img width="322" height="515" alt="43" src="https://github.com/user-attachments/assets/be8e2be7-04dd-49e9-be95-378cc73a120a" />

_________________________________________
44
Write a SQL code to find output table as below:
employeeid, employee_default_phone_number, total_entry, total_login, total_logout, latest_login, latest_logout.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
CREATE TABLE employee_checkin_details
(employeeid INT, entry_details VARCHAR(20),
timestamp_details DATETIME);

CREATE TABLE employee_details
(employeeid INT, phone_number VARCHAR(20), isdefault VARCHAR(20));

INSERT INTO employee_checkin_details
VALUES 
(1000 , 'login' , '2023-06-16 01:00:15.34'),
 (1000 , 'login' , '2023-06-16 02:00:15.34'),
 (1000 , 'login' , '2023-06-16 03:00:15.34'),
 (1000 , 'logout' , '2023-06-16 12:00:15.34'),
 (1001 , 'login' , '2023-06-16 01:00:15.34'),
 (1001 , 'login' , '2023-06-16 02:00:15.34'),
 (1001 , 'login' , '2023-06-16 03:00:15.34'),
 (1001 , 'logout' , '2023-06-16 12:00:15.34');

INSERT INTO employee_details
VALUES 
(1001 ,'9999' , 'false'),
(1001 ,'1111' , 'false'),
(1001 ,'2222' , 'true'),
(1003 ,'3333' , 'false');

<img width="367" height="511" alt="44" src="https://github.com/user-attachments/assets/220a571e-f09d-4002-ad0c-18cbe62b374d" />

____________________________________________________________________________-
45
An organization is looking to hire employees/candidates for their junior and senior positions. They have a total quota/limit of $50000 in all,
they have to first fill up the senior positions and then fill up the junior positions. There are 4 test cases. Write a SQL Query to satisfy all
the testcases. To check whether your SQL query is correct or wrong, you can try with your own test case too.

Create table candidates(
id int primary key,
positions varchar(10) not null,
salary int not null);

-- Test case 1
insert into candidates values(1,'junior',5000);
insert into candidates values(2,'junior',7000);
insert into candidates values(3,'junior',7000);
insert into candidates values(4,'senior',10000);
insert into candidates values(5,'senior',30000);
insert into candidates values(6,'senior',20000);

-- Test case 2
insert into candidates values(20,'junior',10000);
insert into candidates values(30,'senior',15000);
insert into candidates values(40,'senior',30000);

-- Test case 3
insert into candidates values(1,'junior',15000);
insert into candidates values(2,'junior',15000);
insert into candidates values(3,'junior',20000);
insert into candidates values(4,'senior',60000);

-- Test case 4
insert into candidates values(10,'junior',10000);
insert into candidates values(40,'junior',10000);
insert into candidates values(20,'senior',15000);
insert into candidates values(30,'senior',30000);
insert into candidates values(50,'senior',15000);

<img width="292" height="518" alt="45" src="https://github.com/user-attachments/assets/2345b6a8-d3fd-4fa1-bd27-8b50facd2988" />

______________________________________________________
46
Write a SQL query to generate a list of all job positions, including multiple entries for each position based on the number of available posts.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table job_positions (id int,
 title varchar(100),
 groups varchar(10),
 levels varchar(10), 
 payscale int, 
 totalpost int );
 insert into job_positions values (1, 'General manager', 'A', 'l-15', 10000, 1); 
insert into job_positions values (2, 'Manager', 'B', 'l-14', 9000, 5); 
insert into job_positions values (3, 'Asst. Manager', 'C', 'l-13', 8000, 10); 

 create table job_employees ( id int, 
 name  varchar(100), 
 position_id int 
 ); 
 insert into job_employees values (1, 'John Smith', 1); 
insert into job_employees values (2, 'Jane Doe', 2);
 insert into job_employees values (3, 'Michael Brown', 2);
 insert into job_employees values (4, 'Emily Johnson', 2); 
insert into job_employees values (5, 'William Lee', 3); 
insert into job_employees values (6, 'Jessica Clark', 3); 
insert into job_employees values (7, 'Christopher Harris', 3);
 insert into job_employees values (8, 'Olivia Wilson', 3);
 insert into job_employees values (9, 'Daniel Martinez', 3);
 insert into job_employees values (10, 'Sophia Miller', 3);

 <img width="290" height="491" alt="46" src="https://github.com/user-attachments/assets/9aecdad2-8bfb-4d19-a2d7-4aed2a13e27b" />

_______________________________________________________________________

Write a SQL Query to find cities where not even a single order was returned.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table namaste_orders
(
order_id int,
city varchar(10),
sales int
);

create table namaste_returns
(
order_id int,
return_reason varchar(20),
);

insert into namaste_orders
values(1, 'Mysore' , 100),(2, 'Mysore' , 200),(3, 'Bangalore' , 250),(4, 'Bangalore' , 150)
,(5, 'Mumbai' , 300),(6, 'Mumbai' , 500),(7, 'Mumbai' , 800)
;
insert into namaste_returns values
(3,'wrong item'),(6,'bad quality'),(7,'wrong item');

<img width="410" height="465" alt="47" src="https://github.com/user-attachments/assets/54cdd230-871c-410b-9b09-180f43056501" />

____________________________________
48
Write a SQL Query to find the start and final destination.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
CREATE TABLE travel_data (
 customer VARCHAR(10),
 start_loc VARCHAR(50),
 end_loc VARCHAR(50)
);

INSERT INTO travel_data (customer, start_loc, end_loc) VALUES
 ('c1', 'New York', 'Lima'),
 ('c1', 'London', 'New York'),
 ('c1', 'Lima', 'Sao Paulo'),
 ('c1', 'Sao Paulo', 'New Delhi'),
 ('c2', 'Mumbai', 'Hyderabad'),
 ('c2', 'Surat', 'Pune'),
 ('c2', 'Hyderabad', 'Surat'),
 ('c3', 'Kochi', 'Kurnool'),
 ('c3', 'Lucknow', 'Agra'),
 ('c3', 'Agra', 'Jaipur'),
 ('c3', 'Jaipur', 'Kochi');

 <img width="437" height="517" alt="48" src="https://github.com/user-attachments/assets/2a936baa-bca7-413c-ad5a-c80faf028dd8" />


________________________________________________________________
49
Write a SQL Query to find the price at the start of the month and the difference. Make sure you already have the Calendar Table created.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table sku 
(
sku_id int,
price_date date ,
price int
);
insert into sku values 
(1,'2023-01-01',10)
,(1,'2023-02-15',15)
,(1,'2023-03-03',18)
,(1,'2023-03-27',15)
,(1,'2023-04-06',20);

<img width="425" height="496" alt="49 1" src="https://github.com/user-attachments/assets/015b07a5-7027-4426-9db5-d1495b5e2890" />
<br>
<img width="545" height="261" alt="49 2" src="https://github.com/user-attachments/assets/0dfdd597-3b95-4b84-8367-e96a98221d2f" />



______________________________________________________________________________________________________________________________________-
50)
Write a SQL Query to find the firstname, middlename, and lastname from the customer name.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table customers (customer_name varchar(30));

insert into customers values ('Ankit Bansal')
,('Vishal Pratap Singh')
,('Michael'); 
<img width="479" height="473" alt="50" src="https://github.com/user-attachments/assets/d47e93d1-7598-4318-82d7-6493b8c712cd" />


_________________________________________________________________________________________________________________

51
n the last 7 days, get a distribution of games (% of total games) based on the social interaction that is happening during the games. 
Please consider the following as the categories for getting the distribution:
 - No Social Interaction (No message, emojis or gifts sent during the game).
 - One sided interaction (Messages, emojis or gifts sent during the game by only one player).
 - Both sided interaction without custom_typed messages.
 - Both sided interaction with custom_typed_messages from at least one player

Use this below data to create table:-
CREATE TABLE user_interactions (
 user_id varchar(10),
 event varchar(15),
 event_date DATE,
 interaction_type varchar(15),
 game_id varchar(10),
 event_time TIME
);

<img width="417" height="497" alt="51 1" src="https://github.com/user-attachments/assets/00ab98e3-8f59-4bbb-b097-8231816a7a7a" />
<br>
<img width="411" height="361" alt="51 2" src="https://github.com/user-attachments/assets/fe7bc018-2a2e-4e90-8dda-db685295bfa9" />

_________________________________________________________________________________________________________________________
52
Write a SQL Query to find the total matches played, number of matches win and number of losses.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);
INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');

<img width="356" height="511" alt="52" src="https://github.com/user-attachments/assets/923870df-dea8-4b23-9faf-d9c56b0e4b20" />

_________________________________________________________________________________________________________________________________________________

53

Write a SQL Query to find the total number of new customers and repeated customers.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);

insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)

<img width="299" height="492" alt="53" src="https://github.com/user-attachments/assets/7d1fa34a-9cd9-4a73-a8b2-0c3fbfa1808a" />

__________________________________________________________________________________________________________________
54

Write a SQL Query to get the total_visits, most_visited_floor, and resources_used.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));

insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR');

<img width="272" height="507" alt="54" src="https://github.com/user-attachments/assets/699e0c97-380a-4c16-9727-4266ece663da" />

________________________________________________________________________________________________________________________
55
Write a query to find PersonID, Name, number of friends, sum of marks of person who have friends with total score greater than 100.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
Create table friend (pid int, fid int)
insert into friend (pid , fid ) values ('1','2');
insert into friend (pid , fid ) values ('1','3');
insert into friend (pid , fid ) values ('2','1');
insert into friend (pid , fid ) values ('2','3');
insert into friend (pid , fid ) values ('3','5');
insert into friend (pid , fid ) values ('4','2');
insert into friend (pid , fid ) values ('4','3');
insert into friend (pid , fid ) values ('4','5');

create table person (PersonID int, Name varchar(50), Score int)
insert into person(PersonID,Name ,Score) values('1','Alice','88')
insert into person(PersonID,Name ,Score) values('2','Bob','11')
insert into person(PersonID,Name ,Score) values('3','Devis','27')
insert into person(PersonID,Name ,Score) values('4','Tara','45')
insert into person(PersonID,Name ,Score) values('5','John','63')

<img width="375" height="518" alt="55" src="https://github.com/user-attachments/assets/f7fa990d-f994-405f-8147-583d5c2f5c88" />

______________________________________________________________________________________________________________
56
Write a SQL Query to find the cancellation rate of requests with unbanned users (both client and driver must not be banned) each day
between "2013-10-01" and "2013-10-03". Round cancellation rate to two decimal points.
The cancellation rate is computed by dividing the number of cancelled (by client or driver) requests with unbanned users 
by the total number of requests with unbanned users on that day.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
Create table Trips (id int, client_id int, driver_id int, city_id int, status varchar(50), request_at varchar(50));
Create table Users (users_id int, banned varchar(50), role varchar(50));
Truncate table Trips;
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('1', '1', '10', '1', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('2', '2', '11', '1', 'cancelled_by_driver', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('3', '3', '12', '6', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('4', '4', '13', '6', 'cancelled_by_client', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('5', '1', '10', '1', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('6', '2', '11', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('7', '3', '12', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('8', '2', '12', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('9', '3', '10', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('10', '4', '13', '12', 'cancelled_by_driver', '2013-10-03');

<img width="383" height="481" alt="56" src="https://github.com/user-attachments/assets/bb074c1a-8050-43b1-8a2b-dfbe62718277" />

_________________________________________________________________________________________________-
57
Write a SQL Query to find the winner in each group.
The winner in each group is the player who scored the maximum total points within the group. In the case of tie, the lowest player_id wins. 

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table players
(player_id int,
group_id int)

insert into players values (15,1);
insert into players values (25,1);
insert into players values (30,1);
insert into players values (45,1);
insert into players values (10,2);
insert into players values (35,2);
insert into players values (50,2);
insert into players values (20,3);
insert into players values (40,3);

create table matches
(
match_id int,
first_player int,
second_player int,
first_score int,
second_score int)

insert into matches values (1,15,45,3,0);
insert into matches values (2,30,25,1,2);
insert into matches values (3,30,15,2,0);
insert into matches values (4,40,20,5,2);
insert into matches values (5,35,50,1,1);

<img width="317" height="518" alt="57" src="https://github.com/user-attachments/assets/60bd3eab-e33e-4c1e-98a5-692ee5c9a522" />

_______________________________________________________________________-
58
Market Analysis : Write a SQL Query to find for each seller, whether the brand of the second item (by date) they sold is their favorite brand or not.
If the seller sold less than two items, report the answer for that seller as No.

seller_id  2nd_item_fav_brand
1       yes/no
2       yes/no

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using hashtag#Pandas or a
hashtag#PySparkUDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table users (
user_id int ,
 join_date date ,
 favorite_brand varchar(50));

 create table orders (
 order_id int ,
 order_date date ,
 item_id int ,
 buyer_id int ,
 seller_id int 
 );
 create table items
 (
 item_id int ,
 item_brand varchar(50)
 );

 insert into users values (1,'2019-01-01','Lenovo'),(2,'2019-02-09','Samsung'),(3,'2019-01-19','LG'),(4,'2019-05-21','HP');
 insert into items values (1,'Samsung'),(2,'Lenovo'),(3,'LG'),(4,'HP');
 insert into orders values (1,'2019-08-01',4,1,2),(2,'2019-08-02',2,1,3),(3,'2019-08-03',3,2,3),(4,'2019-08-04',1,4,2)
 ,(5,'2019-08-04',1,3,4),(6,'2019-08-05',2,2,4);

 <img width="326" height="521" alt="58" src="https://github.com/user-attachments/assets/35e31345-1c58-4715-9748-3e00e61a8ab8" />

________________________________________________________________________________________________________________________
59
Write a SQL query to find the start and end dates with the state of those tasks where the state is consecutively the same until it changes.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using Pandas or a PySpark UDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table tasks (
date_value date,
state varchar(10)
);

insert into tasks values ('2019-01-01','success'),('2019-01-02','success'),('2019-01-03','success'),('2019-01-04','fail')
,('2019-01-05','fail'),('2019-01-06','success');

<img width="367" height="512" alt="59" src="https://github.com/user-attachments/assets/6877eeb3-31fd-4dfa-bd23-f535daab2e5f" />

______________________________________________________________________________________________________-
60
User Purchase platforms.
-- The table logs the spendings history of users that make purchases from an online shopping website which has a desktop
and a mobile application.
-- Write a SQL Query to find the total number of users and the total amount spent using mobile only, desktop only and 
both mobile and desktop together for each date.

I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

Would you take a different approach? Maybe using Pandas or a PySpark UDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table spending 
(
user_id int,
spend_date date,
platform varchar(10),
amount int
);

insert into spending values(1,'2019-07-01','mobile',100),(1,'2019-07-01','desktop',100),(2,'2019-07-01','mobile',100)
,(2,'2019-07-02','mobile',100),(3,'2019-07-01','desktop',100),(3,'2019-07-02','desktop',100);

<img width="302" height="519" alt="60" src="https://github.com/user-attachments/assets/bdcc5206-1ab4-4eb9-b70e-f03742a7986f" />

_________________________________________________________________________________

