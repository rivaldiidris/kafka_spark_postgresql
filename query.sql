CREATE TABLE postgres.loan (
	loan_id varchar(255) NULL,
	loan_amount int NULL,
	borrower_id varchar(255) NULL,
	status varchar(255) NULL,
	partner varchar(255) NULL,
	current_dpd int NULL,
	max_dpd int NULL,
	interest_rate double precision NULL,
	loan_term int NULL
);

CREATE TABLE postgres.partner (
	partner varchar(255) NULL,
	name varchar(255) NULL
);


-- Find the Top 2 partners with the highest disbursement.
WITH Grouping_Table AS (
	SELECT
		partner,
		SUM(loan_amount) as Total_Disbursement
	FROM loan
	WHERE status = 'CLOSED' OR status = 'LIVE'
	GROUP BY 1
	ORDER BY 2 DESC
)
SELECT
	*,
	RANK() OVER( ORDER BY Total_Disbursement DESC) as Ranking
FROM Grouping_Table 
LIMIT 2 ;

-- Show a list of only the second-last loans from each partner. 
WITH Ranking_Table AS (
	SELECT 
		loan_id,
		partner,
		RANK() OVER( PARTITION BY partner ORDER BY loan_id DESC ) as Ranking
	FROM loan
)
SELECT
	*
FROM Ranking_Table
WHERE Ranking = 1 OR Ranking = 2 ;

-- Calculate the average loan term for each partner with the status: CANCEL, CLOSED & SUBMITTED.
SELECT
	partner,
	ROUND(AVG(loan_term), 2) as AVG_LoanTerm
FROM loan
WHERE status = 'CANCELLED' OR status = 'CLOSED' OR status = 'SUBMITTED' 
GROUP BY 1;

-- Total number of loans each partner takes. (Provide partner, partner name, and a number of loans.)
WITH LoanPartnerCount as (
	SELECT
		partner,
		COUNT(loan_id) as TotalCountLoans
	FROM loan
	GROUP BY 1
)
SELECT
	lpc.partner,
	p.name as partner_name,
	lpc.TotalCountLoans
FROM LoanPartnerCount lpc 
LEFT JOIN partner p ON lpc.partner = p.partner ;

-- Calculate the percentage of late loans.
WITH Loans AS (
	SELECT 
		loan_id,
		current_dpd,
		status
	FROM loan 
	-- 	WHERE status = 'CLOSED' OR status = 'LIVE'
), TotalLoans AS (
	SELECT 
		CAST(COUNT(*) AS FLOAT)	
	FROM Loans
), LateLoans AS (
	SELECT 
		CAST(COUNT(*) AS FLOAT)
	FROM Loans
	WHERE current_dpd > 0
)
SELECT ((SELECT * FROM LateLoans) / (SELECT * FROM TotalLoans) * 100) as PercentageLateLoans;

-- Calculate the sum of the loan amount under partner ==”BANK_A.” 
SELECT
	partner,
	SUM(loan_amount) as TotalLoansAmount
FROM loan
GROUP BY 1 
HAVING partner = 'BANK_A';