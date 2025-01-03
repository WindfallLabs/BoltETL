# Datasource Processing Guide

_IMPORTANT: All filenames in the 'raw' folder should begin with a 6-digit 'yearmonth' (e.g. 202412)_


## RideCheck+ (a.k.a. "RCP" or "RC+") 
Login to virtual machine.

Process:
- 


## CleverReports CR0174
[Web Interface](http://10.235.25.14:8888/logon.i4)  


Process:
- NEW INTERFACE: Navigate to the "hamburger" in the top left
- Select "Browse" and "Reports"
- Click "My Favourites" on the left and choose the "CR-0174" report
    - If it's not listed there, search for "0174" in the searchbar
- Set the Year Month field ("YYYYMM" format)
- Set the Schedule Name ("weekday", "saturday", "sunday")
- Use the Up-Arrow icon at top to save report as "CSV" (default options)
- The file should go to your Downloads folder
- Rename the file so that it begins with the year-month and Schedule Name / Service Day
    - e.g. "202411-weekday-CR-0174 Incident Adjusted Distance and Time - Dec 10 2024.csv"
- Move it to the CR-0174 raw data folder with all the rest
    - `C:\Workspace\tmpdb\Data\raw\CR - CR0174`


## Via: Rider Account
[Via Data Generator](https://mnl-mnt.voc.ridewithvia.com/data-generator)

_CAUTION: Sensitive and personal user data_

Login:
- Log in with username, password, and key generated from Yubico Authenticator
    - Via Ops Center
    - Calculate -> press Yubikey
    - Copy to Clipboard and paste in webbrowser input

Process:
- On the right, choose Analytics > Data Generator
- Change the Report at the top using the dropdown to "NTD S-10"
- Make sure there are no filters
- Download as Excel
- Rename to only "Rider Account.xlsx"
- There should only ever be one file, so overwrite when moving to raw data folder
    - `C:\Workspace\tmpdb\Data\raw\Via - Rider Account`


## Via: NTD S-10
Process:
- See "Via: Rider Account" for Login info
- On the right, choose Analytics > Data Generator
- Change the Report at the top using the dropdown to "NTD S-10"
- Set the calendar range
- Download as Excel (CSV is broken on Firefox)

NOTE: the calendar input is janky, CHECK YOUR DATES TWICE!


## Via: Driver Shifts
Process:
- See "Via: Rider Account" for Login info
- Similar as instructions for Via: NTD S-10: change calendar range


## Via: Ride Requests
Process:
- See "Via: Rider Account" for Login info
- Similar as instructions for Via: NTD S-10: change calendar range
