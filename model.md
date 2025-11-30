
<img width="466" height="309" alt="1" src="https://github.com/user-attachments/assets/a4feffc8-0659-4e6e-b399-193a6122c4c9" />

<br>


<img width="433" height="251" alt="2" src="https://github.com/user-attachments/assets/5605ea88-6461-4c8d-98d2-adc9e38412a8" />
<br>
Bushiness key or natural key (how it is made (sug4025--->sug indicate suger    40------> indicate per kg price and    25 ------>indicate expiry)


<img width="466" height="300" alt="3" src="https://github.com/user-attachments/assets/21b78a33-304c-4df5-a8f0-76d70b2a1075" />
problem we are going to face whhen i am going to bushiness key as surogate key
<br>

<img width="485" height="289" alt="4" src="https://github.com/user-attachments/assets/a9380e40-a1af-4e66-b1cb-3ed5e3826092" />
<br>
suppose when two comapny merge then bushinekkey key can not be used as primary key because here in dmart bushiness key is made from 2 letter of name and reliance it is 4 letter

<br>
<img width="470" height="344" alt="5" src="https://github.com/user-attachments/assets/82809c35-3381-49d0-9d3f-fcf99a0c01ec" />
<br>
<img width="440" height="205" alt="6" src="https://github.com/user-attachments/assets/9175f7e7-55e7-400d-8883-cfa75a75ef67" />
<br>
<img width="525" height="212" alt="7" src="https://github.com/user-attachments/assets/d0c696ae-bbc2-4d2b-9992-7865f13cbb41" />
<br>

dimension  table mostly not chsnge frequently with time
<img width="421" height="206" alt="9" src="https://github.com/user-attachments/assets/06f09872-b8bc-4630-916b-b19f8202322a" />

<br>
<img width="581" height="287" alt="10" src="https://github.com/user-attachments/assets/d7703937-b67c-41a1-92d9-8193ce0d2332" />
scd2
<br>
<img width="355" height="276" alt="11" src="https://github.com/user-attachments/assets/d1e04ec4-4caa-4729-bb99-d358f2c3cdec" />

<br>
<br>
___________________________________________________________________________
<br>
Salting
<img width="613" height="311" alt="Screenshot 2025-11-29 132028" src="https://github.com/user-attachments/assets/bf495d93-e062-4f9d-b3dd-dbaddc234e37" />

<br>
<br>
<img width="742" height="392" alt="Screenshot 2025-11-29 124237" src="https://github.com/user-attachments/assets/0e083e78-4b44-4a8c-8684-556f4e2bbc0e" />
<br>
<img width="820" height="373" alt="Screenshot 2025-11-29 124541" src="https://github.com/user-attachments/assets/0fb234c8-8eef-46d0-ad99-70b78b4d9f2f" />
<br>
<img width="704" height="325" alt="Screenshot 2025-11-29 125030" src="https://github.com/user-attachments/assets/d109f859-6e9f-4ca9-adf5-b817635ba6e2" />
<br>
<img width="538" height="357" alt="Screenshot 2025-11-29 125653" src="https://github.com/user-attachments/assets/e86473f0-7e2c-4f09-b591-bdeb6eb2b302" />
<br>

<img width="491" height="311" alt="Screenshot 2025-11-29 125837" src="https://github.com/user-attachments/assets/8e0869a4-2c27-4182-9a7f-370fdbb9c63c" />
<br>
<img width="498" height="326" alt="Screenshot 2025-11-29 130131" src="https://github.com/user-attachments/assets/8ae1d421-16ec-49a5-8655-1aecdf56b3fd" />
<br>
<img width="452" height="302" alt="Screenshot 2025-11-29 130739" src="https://github.com/user-attachments/assets/dbe5636d-ad03-498b-a65d-af633a08300a" />
<br>
_________________________________________________________________________________________________________________

<br>
SCD Data:-

customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

<br>
<img width="509" height="374" alt="Screenshot 2025-11-29 231409" src="https://github.com/user-attachments/assets/58e9c5c4-08ba-451b-a185-9ebe941c0cc3" />
<br>
<img width="419" height="318" alt="Screenshot 2025-11-29 231831" src="https://github.com/user-attachments/assets/99b1c7a4-1a8d-4067-a8e6-9285ab4080ce" />
<br>
<img width="478" height="270" alt="Screenshot 2025-11-29 232008" src="https://github.com/user-attachments/assets/db57f8e5-d95b-4bf1-abf1-6ac1d5124363" />
<br>
<img width="442" height="257" alt="Screenshot 2025-11-29 232053" src="https://github.com/user-attachments/assets/cdc993eb-3f16-4d6f-8ff2-5c491c22208e" />
<br>
<img width="502" height="355" alt="Screenshot 2025-11-29 232213" src="https://github.com/user-attachments/assets/ce8ab6d5-c041-4340-a9dd-9464068eaaaa" />
<br>
<img width="415" height="254" alt="Screenshot 2025-11-29 232924" src="https://github.com/user-attachments/assets/c8e3f90e-e5ff-41f4-afff-b359a7eefa6f" />
<br>
<img width="429" height="224" alt="Screenshot 2025-11-29 233057" src="https://github.com/user-attachments/assets/eb056ab1-2aca-4ebf-87cb-a4563ad2ec74" />
___________________________________________________________________________________















