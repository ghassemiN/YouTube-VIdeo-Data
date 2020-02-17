**This project is part of M. A. Thesis:**


**Communicating “CRISPR Cas-9” through online videos on YouTube**


**Pouria Nazemi**


**Concordia university**


**2020**



**It has two parts:**

**Part one:** 
Get list of the videos:

In the first part, we take all the videos that contain "crispr cas9", "crispr/cas9", "crispr-cas9", "crispr_cas9" or "crispr+cas9" in the title and description, using Youtube Data API.

Due to the limitation and uncertainty of the API total result,I had to run this script on several servers.

For this reason, I have defined another table (date_list) to hold search start and end dates.

date_list table has a column called is_done, which specifies whether or not this row (start and end date) is already taken.
