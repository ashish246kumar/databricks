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

_________________________________________________________________________________

