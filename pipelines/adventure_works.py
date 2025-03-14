import dlt
import sys
import typing as t

from dotenv import load_dotenv

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from requests import Response

class ODataLinkPaginator(JSONLinkPaginator):
    def __init__(
        self,
        next_url_path = "@odata.nextLink",
    ):
        super().__init__()
        self.next_url_path = next_url_path

    def update_state(self, response: Response, data: t.Optional[t.List[t.Any]] = None) -> None:
        """Extracts the next page URL from the JSON response."""
        self._next_reference = response.json().get(self.next_url_path)
        print(f"Next page URL: {self._next_reference}")

@dlt.source(name="adventure_works")
def adventure_works_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/adventureworks/odata/v1/",
        },
        "resource_defaults": {
            "write_disposition": "append",
            "max_table_nesting": 0,
            "endpoint": {
                "data_selector": "value",
                "params": {
                    "$count": "false",
                    "$orderby": "ModifiedDate",
                },
                "incremental": {
                    "start_param": "$filter",
                    "cursor_path": "ModifiedDate",
                    "initial_value": "1970-01-01",
                    "convert": lambda date: f"ModifiedDate ge {date}",
                    "range_start": "closed",
                    "on_cursor_value_missing": "include",
                    "row_order": "asc",
                },
            },
        },
        "resources": [
            {
                "name": "get__adventure_works__odata__v1__departments",
                "table_name": "raw__adventure_works__departments",
                "primary_key": "DepartmentId",
                "endpoint": {
                    "path": "/Departments",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employees",
                "table_name": "raw__adventure_works__employees",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Employees",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employee_pay_histories",
                "table_name": "raw__adventure_works__employee_pay_histories",
                "primary_key": ["BusinessEntityId", "RateChangeDate"],
                "endpoint": {
                    "path": "/EmployeePayHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__employee_department_histories",
                "table_name": "raw__adventure_works__employee_department_histories",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/EmployeeDepartmentHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__job_candidates",
                "table_name": "raw__adventure_works__job_candidates",
                "primary_key": "JobCandidateId",
                "endpoint": {
                    "path": "/JobCandidates",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__shifts",
                "table_name": "raw__adventure_works__shifts",
                "primary_key": "ShiftId",
                "endpoint": {
                    "path": "/Shifts",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__addresses",
                "table_name": "raw__adventure_works__addresses",
                "primary_key": "AddressId",
                "endpoint": {
                    "path": "/Addresses",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__address_types",
                "table_name": "raw__adventure_works__address_types",
                "primary_key": "AddressTypeId",
                "endpoint": {
                    "path": "/AddressTypes",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            # {
            #     "name": "get__adventure_works__odata__v1__business_entities",
            #     "table_name": "raw__adventure_works__business_entities",
            #     "primary_key": "BusinessEntityId",
            #     "endpoint": {
            #         "path": "/BusinessEntities",
            #         "paginator": ODataLinkPaginator("@odata.nextLink"),
            #     },
            # },
            {
                "name": "get__adventure_works__odata__v1__business_entity_addresses",
                "table_name": "raw__adventure_works__business_entity_addresses",
                "primary_key": "AddressId",
                "endpoint": {
                    "path": "/BusinessEntityAddresses",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__business_entity_contacts",
                "table_name": "raw__adventure_works__business_entity_contacts",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/BusinessEntityContacts",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__contact_types",
                "table_name": "raw__adventure_works__contact_types",
                "primary_key": "ContactTypeId",
                "endpoint": {
                    "path": "/ContactTypes",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__country_regions",
                "table_name": "raw__adventure_works__country_regions",
                "primary_key": "CountryRegionCode",
                "endpoint": {
                    "path": "/CountryRegions",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__email_addresses",
                "table_name": "raw__adventure_works__email_addresses",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/EmailAddresses",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__person_phones",
                "table_name": "raw__adventure_works__person_phones",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/PersonPhones",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__phone_number_types",
                "table_name": "raw__adventure_works__phone_number_types",
                "primary_key": "PhoneNumberTypeId",
                "endpoint": {
                    "path": "/PhoneNumberTypes",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__persons",
                "table_name": "raw__adventure_works__persons",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Persons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__state_provinces",
                "table_name": "raw__adventure_works__state_provinces",
                "primary_key": "StateProvinceId",
                "endpoint": {
                    "path": "/StateProvinces",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__bill_of_materials",
                "table_name": "raw__adventure_works__bill_of_materials",
                "primary_key": "BillOfMaterialsId",
                "endpoint": {
                    "path": "/BillOfMaterials",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__cultures",
                "table_name": "raw__adventure_works__cultures",
                "primary_key": "CultureId",
                "endpoint": {
                    "path": "/Cultures",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__illustrations",
                "table_name": "raw__adventure_works__illustrations",
                "primary_key": "IllustrationId",
                "endpoint": {
                    "path": "/Illustrations",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__locations",
                "table_name": "raw__adventure_works__locations",
                "primary_key": "LocationId",
                "endpoint": {
                    "path": "/Locations",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_categories",
                "table_name": "raw__adventure_works__product_categories",
                "primary_key": "ProductCategoryId",
                "endpoint": {
                    "path": "/ProductCategories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_cost_histories",
                "table_name": "raw__adventure_works__product_cost_histories",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "/ProductCostHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_descriptions",
                "table_name": "raw__adventure_works__product_descriptions",
                "primary_key": "ProductDescriptionId",
                "endpoint": {
                    "path": "/ProductDescriptions",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_inventories",
                "table_name": "raw__adventure_works__product_inventories",
                "primary_key": "LocationId",
                "endpoint": {
                    "path": "/ProductInventories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_list_price_histories",
                "table_name": "raw__adventure_works__product_list_price_histories",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "/ProductListPriceHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_model_illustrations",
                "table_name": "raw__adventure_works__product_model_illustrations",
                "primary_key": "IllustrationId",
                "endpoint": {
                    "path": "/ProductModelIllustrations",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_models",
                "table_name": "raw__adventure_works__product_models",
                "primary_key": "ProductModelId",
                "endpoint": {
                    "path": "/ProductModels",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_photos",
                "table_name": "raw__adventure_works__product_photos",
                "primary_key": "ProductPhotoId",
                "endpoint": {
                    "path": "/ProductPhotos",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_reviews",
                "table_name": "raw__adventure_works__product_reviews",
                "primary_key": "ProductReviewId",
                "endpoint": {
                    "path": "/ProductReviews",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__products",
                "table_name": "raw__adventure_works__products",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "/Products",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_subcategories",
                "table_name": "raw__adventure_works__product_subcategories",
                "primary_key": "ProductSubcategoryId",
                "endpoint": {
                    "path": "/ProductSubcategories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__scrap_reasons",
                "table_name": "raw__adventure_works__scrap_reasons",
                "primary_key": "ScrapReasonId",
                "endpoint": {
                    "path": "/ScrapReasons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__transaction_histories",
                "table_name": "raw__adventure_works__transaction_histories",
                "primary_key": "TransactionId",
                "endpoint": {
                    "path": "/TransactionHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__transaction_history_archives",
                "table_name": "raw__adventure_works__transaction_history_archives",
                "primary_key": "TransactionId",
                "endpoint": {
                    "path": "/TransactionHistoryArchives",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__unit_measures",
                "table_name": "raw__adventure_works__unit_measures",
                "primary_key": "UnitMeasureCode",
                "endpoint": {
                    "path": "/UnitMeasures",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__work_orders",
                "table_name": "raw__adventure_works__work_orders",
                "primary_key": "WorkOrderId",
                "endpoint": {
                    "path": "/WorkOrders",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__work_order_routings",
                "table_name": "raw__adventure_works__work_order_routings",
                "primary_key": "OperationSequence",
                "endpoint": {
                    "path": "/WorkOrderRoutings",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__product_vendors",
                "table_name": "raw__adventure_works__product_vendors",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/ProductVendors",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__purchase_order_details",
                "table_name": "raw__adventure_works__purchase_order_details",
                "primary_key": "PurchaseOrderDetailId",
                "endpoint": {
                    "path": "/PurchaseOrderDetails",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__purchase_order_headers",
                "table_name": "raw__adventure_works__purchase_order_headers",
                "primary_key": "PurchaseOrderId",
                "endpoint": {
                    "path": "/PurchaseOrderHeaders",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__ship_methods",
                "table_name": "raw__adventure_works__ship_methods",
                "primary_key": "ShipMethodId",
                "endpoint": {
                    "path": "/ShipMethods",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__vendors",
                "table_name": "raw__adventure_works__vendors",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Vendors",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__credit_cards",
                "table_name": "raw__adventure_works__credit_cards",
                "primary_key": "CreditCardId",
                "endpoint": {
                    "path": "/CreditCards",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__currencies",
                "table_name": "raw__adventure_works__currencies",
                "primary_key": "CurrencyCode",
                "endpoint": {
                    "path": "/Currencies",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__currency_rates",
                "table_name": "raw__adventure_works__currency_rates",
                "primary_key": "CurrencyRateId",
                "endpoint": {
                    "path": "/CurrencyRates",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__customers",
                "table_name": "raw__adventure_works__customers",
                "primary_key": "CustomerId",
                "endpoint": {
                    "path": "/Customers",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_headers",
                "table_name": "raw__adventure_works__sales_order_headers",
                "primary_key": "SalesOrderId",
                "endpoint": {
                    "path": "/SalesOrderHeaders",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_order_details",
                "table_name": "raw__adventure_works__sales_order_details",
                "primary_key": "SalesOrderDetailId",
                "endpoint": {
                    "path": "/SalesOrderDetails",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_persons",
                "table_name": "raw__adventure_works__sales_persons",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/SalesPersons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_person_quota_histories",
                "table_name": "raw__adventure_works__sales_person_quota_histories",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/SalesPersonQuotaHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_reasons",
                "table_name": "raw__adventure_works__sales_reasons",
                "primary_key": "SalesReasonId",
                "endpoint": {
                    "path": "/SalesReasons",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_tax_rates",
                "table_name": "raw__adventure_works__sales_tax_rates",
                "primary_key": "SalesTaxRateId",
                "endpoint": {
                    "path": "/SalesTaxRates",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_territories",
                "table_name": "raw__adventure_works__sales_territories",
                "primary_key": "TerritoryId",
                "endpoint": {
                    "path": "/SalesTerritories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__sales_territory_histories",
                "table_name": "raw__adventure_works__sales_territory_histories",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/SalesTerritoryHistories",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__shopping_cart_items",
                "table_name": "raw__adventure_works__shopping_cart_items",
                "primary_key": "ShoppingCartItemId",
                "endpoint": {
                    "path": "/ShoppingCartItems",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__special_offers",
                "table_name": "raw__adventure_works__special_offers",
                "primary_key": "SpecialOfferId",
                "endpoint": {
                    "path": "/SpecialOffers",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
            {
                "name": "get__adventure_works__odata__v1__stores",
                "table_name": "raw__adventure_works__stores",
                "primary_key": "BusinessEntityId",
                "endpoint": {
                    "path": "/Stores",
                    "paginator": ODataLinkPaginator("@odata.nextLink"),
                },
            },
        ]
    }

    yield from rest_api_resources(source_config)

def load_adventure_works(env) -> None:
    dev_mode = env != "prod"
    print(f"Running in {'dev' if dev_mode else 'prod'} mode")
    
    load_dotenv()
    
    pipeline = dlt.pipeline(
        pipeline_name="adventure_works",
        destination=dlt.destinations.filesystem("./lakehouse"),
        dataset_name="das",
        progress="enlighten",
        export_schema_path="./pipelines/schemas/export",
        import_schema_path="./pipelines/schemas/import",
        dev_mode=dev_mode
    )

    source = adventure_works_source()
    
    load_info = pipeline.run(source, table_format="iceberg")
    print(load_info)
    
if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    load_adventure_works(env=env)