WITH "cte__source" AS (                                                                  
  SELECT                                                                                 
    "raw__adventure_works__sales_order_details"."sales_order_id" AS "sales_order_id",    
    "raw__adventure_works__sales_order_details"."sales_order_detail_id" AS               
"sales_order_detail_id",                                                                 
    "raw__adventure_works__sales_order_details"."carrier_tracking_number" AS             
"carrier_tracking_number",                                                               
    "raw__adventure_works__sales_order_details"."order_qty" AS "order_qty",              
    "raw__adventure_works__sales_order_details"."product_id" AS "product_id",            
    "raw__adventure_works__sales_order_details"."special_offer_id" AS "special_offer_id",
    "raw__adventure_works__sales_order_details"."unit_price" AS "unit_price",            
    "raw__adventure_works__sales_order_details"."unit_price_discount" AS                 
"unit_price_discount",                                                                   
    "raw__adventure_works__sales_order_details"."line_total" AS "line_total",            
    "raw__adventure_works__sales_order_details"."rowguid" AS "rowguid",                  
    "raw__adventure_works__sales_order_details"."modified_date" AS "modified_date",      
    TO_TIMESTAMP(                                                                        
      CAST("raw__adventure_works__sales_order_details"."_dlt_load_id" AS DECIMAL(18, 3)) 
    ) AS "record_loaded_at"                                                              
  FROM "db"."sqlmesh__das"."das__raw__adventure_works__sales_order_details__835388414" AS
"raw__adventure_works__sales_order_details" /*                                           
db.das.raw__adventure_works__sales_order_details */                                      
), "cte__scd" AS (                                                                       
  SELECT                                                                                 
    "cte__source"."sales_order_id" AS "sales_order_id",                                  
    "cte__source"."sales_order_detail_id" AS "sales_order_detail_id",                    
    "cte__source"."carrier_tracking_number" AS "carrier_tracking_number",                
    "cte__source"."order_qty" AS "order_qty",                                            
    "cte__source"."product_id" AS "product_id",                                          
    "cte__source"."special_offer_id" AS "special_offer_id",                              
    "cte__source"."unit_price" AS "unit_price",                                          
    "cte__source"."unit_price_discount" AS "unit_price_discount",                        
    "cte__source"."line_total" AS "line_total",                                          
    "cte__source"."rowguid" AS "rowguid",                                                
    "cte__source"."modified_date" AS "modified_date",                                    
    "cte__source"."record_loaded_at" AS "record_loaded_at",                              
    ROW_NUMBER() OVER (PARTITION BY "cte__source"."sales_order_detail_id" ORDER BY       
"cte__source"."record_loaded_at") AS "record_version",                                   
    CASE                                                                                 
      WHEN ROW_NUMBER() OVER (PARTITION BY "cte__source"."sales_order_detail_id" ORDER BY
"cte__source"."record_loaded_at") = 1                                                    
      THEN CAST('1970-01-01 00:00:00' AS TIMESTAMP)                                      
      ELSE "cte__source"."record_loaded_at"                                              
    END AS "record_valid_from",                                                          
    COALESCE(                                                                            
      LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                          
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at"),        
      CAST('9999-12-31 23:59:59' AS TIMESTAMP)                                           
    ) AS "record_valid_to",                                                              
    CASE                                                                                 
      WHEN (                                                                             
        LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                        
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at") =       
CAST('9999-12-31 23:59:59' AS TIMESTAMP)                                                 
        OR LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                     
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at") IS NULL 
      )                                                                                  
      THEN TRUE                                                                          
      ELSE FALSE                                                                         
    END AS "is_current_record",                                                          
    CASE                                                                                 
      WHEN CASE                                                                          
        WHEN (                                                                           
          LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                      
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at") =       
CAST('9999-12-31 23:59:59' AS TIMESTAMP)                                                 
          OR LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                   
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at") IS NULL 
        )                                                                                
        THEN TRUE                                                                        
        ELSE FALSE                                                                       
      END                                                                                
      THEN "cte__source"."record_loaded_at"                                              
      ELSE COALESCE(                                                                     
        LEAD("cte__source"."record_loaded_at") OVER (PARTITION BY                        
"cte__source"."sales_order_detail_id" ORDER BY "cte__source"."record_loaded_at"),        
        CAST('9999-12-31 23:59:59' AS TIMESTAMP)                                         
      )                                                                                  
    END AS "record_updated_at"                                                           
  FROM "cte__source" AS "cte__source"                                                    
), "cte__hooks" AS (                                                                     
  SELECT                                                                                 
    CONCAT(                                                                              
      'order_line__sales__adventure_works|',                                             
      CAST("cte__scd"."sales_order_detail_id" AS TEXT)                                   
    ) AS "_hook__order_line__sales",                                                     
    CONCAT('order__sales__adventure_works|', CAST("cte__scd"."sales_order_id" AS TEXT))  
AS "_hook__order__sales",                                                                
    CONCAT('product__adventure_works|', CAST("cte__scd"."product_id" AS TEXT)) AS        
"_hook__product",                                                                        
    CONCAT(                                                                              
      'reference__special_offer__adventure_works|',                                      
      CAST("cte__scd"."special_offer_id" AS TEXT)                                        
    ) AS "_hook__reference__special_offer",                                              
    "cte__scd"."sales_order_id" AS "sales_order_id",                                     
    "cte__scd"."sales_order_detail_id" AS "sales_order_detail_id",                       
    "cte__scd"."carrier_tracking_number" AS "carrier_tracking_number",                   
    "cte__scd"."order_qty" AS "order_qty",                                               
    "cte__scd"."product_id" AS "product_id",                                             
    "cte__scd"."special_offer_id" AS "special_offer_id",                                 
    "cte__scd"."unit_price" AS "unit_price",                                             
    "cte__scd"."unit_price_discount" AS "unit_price_discount",                           
    "cte__scd"."line_total" AS "line_total",                                             
    "cte__scd"."rowguid" AS "rowguid",                                                   
    "cte__scd"."modified_date" AS "modified_date",                                       
    "cte__scd"."record_loaded_at" AS "record_loaded_at",                                 
    "cte__scd"."record_version" AS "record_version",                                     
    "cte__scd"."record_valid_from" AS "record_valid_from",                               
    "cte__scd"."record_valid_to" AS "record_valid_to",                                   
    "cte__scd"."is_current_record" AS "is_current_record",                               
    "cte__scd"."record_updated_at" AS "record_updated_at"                                
  FROM "cte__scd" AS "cte__scd"                                                          
), "cte__composite_hooks" AS (                                                           
  SELECT                                                                                 
    "cte__hooks"."_hook__order_line__sales" AS "_hook__order_line__sales",               
    "cte__hooks"."_hook__order__sales" AS "_hook__order__sales",                         
    "cte__hooks"."_hook__product" AS "_hook__product",                                   
    "cte__hooks"."_hook__reference__special_offer" AS "_hook__reference__special_offer", 
    "cte__hooks"."sales_order_id" AS "sales_order_id",                                   
    "cte__hooks"."sales_order_detail_id" AS "sales_order_detail_id",                     
    "cte__hooks"."carrier_tracking_number" AS "carrier_tracking_number",                 
    "cte__hooks"."order_qty" AS "order_qty",                                             
    "cte__hooks"."product_id" AS "product_id",                                           
    "cte__hooks"."special_offer_id" AS "special_offer_id",                               
    "cte__hooks"."unit_price" AS "unit_price",                                           
    "cte__hooks"."unit_price_discount" AS "unit_price_discount",                         
    "cte__hooks"."line_total" AS "line_total",                                           
    "cte__hooks"."rowguid" AS "rowguid",                                                 
    "cte__hooks"."modified_date" AS "modified_date",                                     
    "cte__hooks"."record_loaded_at" AS "record_loaded_at",                               
    "cte__hooks"."record_version" AS "record_version",                                   
    "cte__hooks"."record_valid_from" AS "record_valid_from",                             
    "cte__hooks"."record_valid_to" AS "record_valid_to",                                 
    "cte__hooks"."is_current_record" AS "is_current_record",                             
    "cte__hooks"."record_updated_at" AS "record_updated_at"                              
  FROM "cte__hooks" AS "cte__hooks"                                                      
), "cte__primary_hooks" AS (                                                             
  SELECT                                                                                 
    CONCAT(                                                                              
      "cte__composite_hooks"."_hook__order_line__sales",                                 
      '~epoch__valid_from|',                                                             
      CAST("cte__composite_hooks"."record_valid_from" AS TEXT)                           
    ) AS "_pit_hook__order_line__sales",                                                 
    "cte__composite_hooks"."_hook__order_line__sales" AS "_hook__order_line__sales",     
    "cte__composite_hooks"."_hook__order__sales" AS "_hook__order__sales",               
    "cte__composite_hooks"."_hook__product" AS "_hook__product",                         
    "cte__composite_hooks"."_hook__reference__special_offer" AS                          
"_hook__reference__special_offer",                                                       
    "cte__composite_hooks"."sales_order_id" AS "sales_order_id",                         
    "cte__composite_hooks"."sales_order_detail_id" AS "sales_order_detail_id",           
    "cte__composite_hooks"."carrier_tracking_number" AS "carrier_tracking_number",       
    "cte__composite_hooks"."order_qty" AS "order_qty",                                   
    "cte__composite_hooks"."product_id" AS "product_id",                                 
    "cte__composite_hooks"."special_offer_id" AS "special_offer_id",                     
    "cte__composite_hooks"."unit_price" AS "unit_price",                                 
    "cte__composite_hooks"."unit_price_discount" AS "unit_price_discount",               
    "cte__composite_hooks"."line_total" AS "line_total",                                 
    "cte__composite_hooks"."rowguid" AS "rowguid",                                       
    "cte__composite_hooks"."modified_date" AS "modified_date",                           
    "cte__composite_hooks"."record_loaded_at" AS "record_loaded_at",                     
    "cte__composite_hooks"."record_version" AS "record_version",                         
    "cte__composite_hooks"."record_valid_from" AS "record_valid_from",                   
    "cte__composite_hooks"."record_valid_to" AS "record_valid_to",                       
    "cte__composite_hooks"."is_current_record" AS "is_current_record",                   
    "cte__composite_hooks"."record_updated_at" AS "record_updated_at"                    
  FROM "cte__composite_hooks" AS "cte__composite_hooks"                                  
), "cte__prefixed" AS (                                                                  
  SELECT                                                                                 
    "cte__primary_hooks"."_pit_hook__order_line__sales" AS                               
"_pit_hook__order_line__sales",                                                          
    "cte__primary_hooks"."_hook__order_line__sales" AS "_hook__order_line__sales",       
    "cte__primary_hooks"."_hook__order__sales" AS "_hook__order__sales",                 
    "cte__primary_hooks"."_hook__product" AS "_hook__product",                           
    "cte__primary_hooks"."_hook__reference__special_offer" AS                            
"_hook__reference__special_offer",                                                       
    "cte__primary_hooks"."sales_order_id" AS "sales_order_detail__sales_order_id",       
    "cte__primary_hooks"."sales_order_detail_id" AS                                      
"sales_order_detail__sales_order_detail_id",                                             
    "cte__primary_hooks"."carrier_tracking_number" AS                                    
"sales_order_detail__carrier_tracking_number",                                           
    "cte__primary_hooks"."order_qty" AS "sales_order_detail__order_qty",                 
    "cte__primary_hooks"."product_id" AS "sales_order_detail__product_id",               
    "cte__primary_hooks"."special_offer_id" AS "sales_order_detail__special_offer_id",   
    "cte__primary_hooks"."unit_price" AS "sales_order_detail__unit_price",               
    "cte__primary_hooks"."unit_price_discount" AS                                        
"sales_order_detail__unit_price_discount",                                               
    "cte__primary_hooks"."line_total" AS "sales_order_detail__line_total",               
    "cte__primary_hooks"."rowguid" AS "sales_order_detail__rowguid",                     
    "cte__primary_hooks"."modified_date" AS "sales_order_detail__modified_date",         
    "cte__primary_hooks"."record_loaded_at" AS "sales_order_detail__record_loaded_at",   
    "cte__primary_hooks"."record_updated_at" AS "sales_order_detail__record_updated_at", 
    "cte__primary_hooks"."record_version" AS "sales_order_detail__record_version",       
    "cte__primary_hooks"."record_valid_from" AS "sales_order_detail__record_valid_from", 
    "cte__primary_hooks"."record_valid_to" AS "sales_order_detail__record_valid_to",     
    "cte__primary_hooks"."is_current_record" AS "sales_order_detail__is_current_record"  
  FROM "cte__primary_hooks" AS "cte__primary_hooks"                                      
)                                                                                        
SELECT                                                                                   
  CAST("cte__prefixed"."_pit_hook__order_line__sales" AS BLOB) AS                        
"_pit_hook__order_line__sales", /* Point in time version of _hook__order_line__sales. */ 
  CAST("cte__prefixed"."_hook__order_line__sales" AS BLOB) AS "_hook__order_line__sales",
/* Primary hook for sales_order_detail_id using keyset:                                  
order_line__sales__adventure_works. */                                                   
  CAST("cte__prefixed"."_hook__order__sales" AS BLOB) AS "_hook__order__sales", /* Hook  
for sales_order_id using keyset: order__sales__adventure_works. */                       
  CAST("cte__prefixed"."_hook__product" AS BLOB) AS "_hook__product", /* Hook for        
product_id using keyset: product__adventure_works. */                                    
  CAST("cte__prefixed"."_hook__reference__special_offer" AS BLOB) AS                     
"_hook__reference__special_offer", /* Hook for special_offer_id using keyset:            
reference__special_offer__adventure_works. */                                            
  CAST("cte__prefixed"."sales_order_detail__sales_order_id" AS BIGINT) AS                
"sales_order_detail__sales_order_id", /* Primary key. Foreign key to                     
SalesOrderHeader.SalesOrderID. */                                                        
  CAST("cte__prefixed"."sales_order_detail__sales_order_detail_id" AS BIGINT) AS         
"sales_order_detail__sales_order_detail_id", /* Primary key. One incremental unique      
number per product sold. */                                                              
  CAST("cte__prefixed"."sales_order_detail__carrier_tracking_number" AS TEXT) AS         
"sales_order_detail__carrier_tracking_number", /* Shipment tracking number supplied by   
the shipper. */                                                                          
  CAST("cte__prefixed"."sales_order_detail__order_qty" AS BIGINT) AS                     
"sales_order_detail__order_qty", /* Quantity ordered per product. */                     
  CAST("cte__prefixed"."sales_order_detail__product_id" AS BIGINT) AS                    
"sales_order_detail__product_id", /* Product sold to customer. Foreign key to            
Product.ProductID. */                                                                    
  CAST("cte__prefixed"."sales_order_detail__special_offer_id" AS BIGINT) AS              
"sales_order_detail__special_offer_id", /* Promotional code. Foreign key to              
SpecialOffer.SpecialOfferID. */                                                          
  CAST("cte__prefixed"."sales_order_detail__unit_price" AS DOUBLE) AS                    
"sales_order_detail__unit_price", /* Selling price of a single product. */               
  CAST("cte__prefixed"."sales_order_detail__unit_price_discount" AS DOUBLE) AS           
"sales_order_detail__unit_price_discount", /* Discount amount. */                        
  CAST("cte__prefixed"."sales_order_detail__line_total" AS DOUBLE) AS                    
"sales_order_detail__line_total", /* Per product subtotal. Computed as UnitPrice * (1 -  
UnitPriceDiscount) * OrderQty. */                                                        
  CAST("cte__prefixed"."sales_order_detail__rowguid" AS TEXT) AS                         
"sales_order_detail__rowguid", /* ROWGUIDCOL number uniquely identifying the record. Used
to support a merge replication sample. */                                                
  CAST("cte__prefixed"."sales_order_detail__modified_date" AS DATE) AS                   
"sales_order_detail__modified_date", /* Date and time the record was last updated. */    
  CAST("cte__prefixed"."sales_order_detail__record_loaded_at" AS TIMESTAMP) AS           
"sales_order_detail__record_loaded_at", /* Timestamp when this record was loaded into the
system */                                                                                
  CAST("cte__prefixed"."sales_order_detail__record_updated_at" AS TIMESTAMP) AS          
"sales_order_detail__record_updated_at", /* Timestamp when this record was last updated  
*/                                                                                       
  CAST("cte__prefixed"."sales_order_detail__record_version" AS INT) AS                   
"sales_order_detail__record_version", /* Version number for this record */               
  CAST("cte__prefixed"."sales_order_detail__record_valid_from" AS TIMESTAMP) AS          
"sales_order_detail__record_valid_from", /* Timestamp from which this record version is  
valid */                                                                                 
  CAST("cte__prefixed"."sales_order_detail__record_valid_to" AS TIMESTAMP) AS            
"sales_order_detail__record_valid_to", /* Timestamp until which this record version is   
valid */                                                                                 
  CAST("cte__prefixed"."sales_order_detail__is_current_record" AS BOOLEAN) AS            
"sales_order_detail__is_current_record" /* Flag indicating if this is the current valid  
version of the record */                                                                 
FROM "cte__prefixed" AS "cte__prefixed"                                                  
WHERE                                                                                    
  "cte__prefixed"."sales_order_detail__record_updated_at" <= '9999-12-31 23:59:59.999999'
  AND "cte__prefixed"."sales_order_detail__record_updated_at" >= '1970-01-01 00:00:00'   
