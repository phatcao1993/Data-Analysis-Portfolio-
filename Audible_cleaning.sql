SELECT *
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned` limit 100 ;

-- getting grid of unimportant text from author and narrator columns 
SELECT 
  SPLIT(author, ":")[OFFSET(1)] AS author_cleaned,
  SPLIT(narrator,":")[OFFSET(1)] AS narrator_cleaned,
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned` ;

ALTER TABLE `marketing-ad-ab-test.Audible.Audible_uncleaned`
ADD COLUMN  author_cleaned STRING,
ADD COLUMN  narrator_cleaned STRING;

UPDATE  `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET author_cleaned = SPLIT(author, ":")[OFFSET(1)]
WHERE author_cleaned is null;

UPDATE  `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET narrator_cleaned = SPLIT(narrator,":")[OFFSET(1)] 
WHERE narrator_cleaned is null;

-- Standardize time column  

SELECT 
 time,
 case
      -- check if data contains hrs and minute ,like 2hrs and 34 mins
      WHEN 
        regexp_contains(time, r'(\d+)\s*hrs\s*and\s*(\d+)\s*min(s)') = true 
        THEN
            (cast(regexp_extract( regexp_extract(time, r'([0-9]+\s*hrs)') ,'[0-9]+') as integer)*60 
              +
              cast(regexp_extract(regexp_extract(time,r'(\d+)\s*mins') ,'[0-9]+') as integer)
            )
      -- check if data contans hr only , like 4 hrs
      WHEN 
        regexp_contains(time, r'(\d+)\s*hr(s)$') = true
        THEN
        cast(regexp_extract( regexp_extract(time, r'^([0-9]+\s*hrs)') ,'[0-9]+') as integer)*60 
      -- check if column contain mins only
      WHEN
        regexp_contains(time, r'^\d+\s*min(s)$')
        THEN
        cast(regexp_extract( regexp_extract(time, r'^([0-9]+\s*min)') ,'[0-9]+') as integer)
      else null
      end
      as Minute   
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned`;

ALTER TABLE `marketing-ad-ab-test.Audible.Audible_uncleaned`
ADD COLUMN Minute INT64;

UPDATE  `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET Minute = 
    case
      -- check if data contains hrs and minute like 2hrs and 34 mins
      WHEN 
        regexp_contains(time, r'(\d+)\s*hrs\s*and\s*(\d+)\s*min(s)') = true 
        THEN
            (cast(regexp_extract( regexp_extract(time, r'([0-9]+\s*hr)') ,'[0-9]+') as integer)*60 
              +
              cast(regexp_extract(regexp_extract(time,r'(\d+)\s*mins') ,'[0-9]+') as integer)
            )
      -- check if data contans hr only , like 4 hrs
      WHEN 
        regexp_contains(time, r'^\s*\d+\s*hr') = true
        THEN
        cast(regexp_extract( regexp_extract(time, r'\s*([0-9]+\s*hr)') ,'[0-9]+') as integer)*60 
      -- check of column contain mins only
      WHEN
        regexp_contains(time, r'^\s*\d+\s*min')
        THEN
        cast(regexp_extract( regexp_extract(time, r'^\s*[0-9]+\s*min') ,'[0-9]+') as integer)
      else 0
      end
WHERE Minute is null;


SELECT distinct time  from `marketing-ad-ab-test.Audible.Audible_uncleaned` where minute is null;


--- seperating number of stars and number of rating from Stars column
--stars


SELECT
  stars,
  CAST (regexp_extract(stars, r'^(\d+\.*\d*)') as FLOAT64) as num_star
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned`;


-- number of rating
SELECT 
  stars,
  CASE 
    WHEN regexp_contains(stars, r'stars[0-9]*') = true
    THEN 
    CAST(regexp_extract(regexp_extract(stars,r'stars[0-9]*'),'[0-9]+') as integer)
  ELSE 0 
  END AS num_rating
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned`;

ALTER TABLE `marketing-ad-ab-test.Audible.Audible_uncleaned`
ADD COLUMN  NUM_STARS       FLOAt64,
ADD COLUMN  NUM_RATING      INT64  ;

 

UPDATE `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET  NUM_STARS = CAST (regexp_extract(stars, r'^(\d+\.*\d*)') as FLOAT64) 
WHERE STARS IS NOT NULL;


UPDATE `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET NUM_RATING = 
      CASE 
      WHEN regexp_contains(stars, r'stars[0-9]*') = true
      THEN 
      CAST(regexp_extract(regexp_extract(stars,r'stars[0-9]*'),'[0-9]+') as integer)
      ELSE NULL
      END 
WHERE NUM_RATING IS NULL;

SELECT *
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned`
WHERE NUM_STARS <=3
ORDER BY NUM_STARS DESC limit 100;

-- Changing Price column to numberic data type 
SELECT 
  case when price in ('Free','free') then 0 
  else cast(replace(price,",","") as float64) 
  end as Price_cleaned
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned`;

ALTER TABLE `marketing-ad-ab-test.Audible.Audible_uncleaned`
ADD COLUMN NEW_PRICE FLOAT64;

UPDATE `marketing-ad-ab-test.Audible.Audible_uncleaned`
SET NEW_PRICE = 
    case when price in ('Free','free') then 0 
    else cast(replace(price,",","") as float64) END
WHERE price is not null;

SELECT *
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned` 
order by NEW_PRICE DESC limit 100 ;

-- checking for duplicate 

with duplicateCTE as 
 (SELECT *,
  row_number() over(partition by name, author, narrator,time, releasedate  order by name ) as row_num
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned` 
  )
select * from duplicateCTE
where  row_num > 1;

-- Creating New Cleaned table
create table audible_cleaned AS 
SELECT 
  name, 
  author_cleaned,
  narrator_cleaned, 
  Minute, NUM_RATING,
  NUM_STARS,
  NEW_PRICE
FROM
  (SELECT *,
  row_number() over(partition by name, author, narrator,time, releasedate  order by name ) as row_num
FROM `marketing-ad-ab-test.Audible.Audible_uncleaned` )
WHERE 
  row_num = 1
