# Data Dictionary — pipeline_data.parquet

## Narrative
This dataset appears to contain daily natural gas scheduling activity at pipeline points across the United States. Each row represents a location-day record with a scheduled volume and metadata about where gas is entering or exiting a pipeline.

- "pipeline_name" identifies the pipeline operator; "loc_name" is the meter/station or delivery point.
- "rec_del_sign" encodes flow direction (−1 receipts into the pipeline, +1 deliveries out of the pipeline).
- "connecting_pipeline" and "connecting_entity" describe the counterparty (another pipeline or an LDC/utility/shipper). "category_short" labels the point type (e.g., LDC, Production, Interconnect, Industrial, Power, Storage).
- Geography includes country/state/county; latitude/longitude are sparsely populated and may require enrichment before mapping.
- "eff_gas_day" is the gas-day date; "scheduled_quantity" is the scheduled volume (units not explicitly stated; treat as heavy‑tailed and validate with aggregates).

Typical uses: track supply (Production) versus demand (LDC/Power), monitor interconnect flows between pipelines, surface anomalies/spikes by region and time, and analyze trend changes. Caveats: many "connecting_pipeline" values are null; lat/long are largely missing; treat outliers carefully and prefer aggregations when forming conclusions.

## Summary
- Rows: 23854855
- Columns: 13
- Time coverage (eff_gas_day): 2022-01-01 → 2025-08-26
- Columns with <80% completeness (sample): connecting_pipeline, latitude, longitude
- Example top categories (sample):
  - pipeline_name: Northern Natural Gas Company (20928), Columbia Gas Transmission, LLC (12151), Enable Gas Transmission (11345), Texas Eastern Transmission, LP (10455), Tennessee Gas Pipeline Company (9421)
  - loc_name: Diversified Production LLC (393), Transco (122), DCP OPER/NGPL KERNS #1 CADDO (72), Florida Gas Transmission (72), SLNG/EEC   CHATHAM (71)
  - connecting_pipeline: None (156875), Tennessee Gas Pipeline Company (2188), Texas Eastern Transmission, LP (1937), Transcontinental Gas Pipe Line Company, LLC (1471), ANR Pipeline Company (1348)

## Columns
| Name | Type | Completeness (sample) | Distinct (approx) | Notes |
|---|---|---:|---:|---|
| pipeline_name | VARCHAR | 100.0% | 165 | top: Northern Natural Gas Company, Columbia Gas Transmission, LLC, Enable Gas Transmission |
| loc_name | VARCHAR | 100.0% | 19387 | top: Diversified Production LLC, Transco, DCP OPER/NGPL KERNS #1 CADDO |
| connecting_pipeline | VARCHAR | 21.8% | 259 | top: None, Tennessee Gas Pipeline Company, Texas Eastern Transmission, LP |
| connecting_entity | VARCHAR | 88.0% | 4536 | top: None, 0, ENABLE GAS TRANSMISSION, |
| rec_del_sign | BIGINT | 100.0% | 2 | min=-1.0, p50=-1.0, p95=1.0, max=1.0 |
| category_short | VARCHAR | 99.9% | 17 | top: LDC, Production, Interconnect |
| country_name | VARCHAR | 100.0% | 1 | top: United States |
| state_abb | VARCHAR | 97.7% | 50 | top: LA, TX, PA |
| county_name | VARCHAR | 90.0% | 1485 | top: None, Jefferson, Greene |
| latitude | INTEGER | 0.0% | 0 | min=None, p50=None, p95=None, max=None |
| longitude | INTEGER | 0.0% | 0 | min=None, p50=None, p95=None, max=None |
| eff_gas_day | DATE | 100.0% | 1395 |  |
| scheduled_quantity | DOUBLE | 99.8% | 593603 | min=0.0, p50=1.0, p95=136034.64999999935, max=233142857.0 |

## Numeric details (sample)
- **rec_del_sign**: min=-1.0, q1=-1.0, median=-1.0, q3=1.0, p95=1.0, max=1.0, mean=-0.2423, std=0.9702013760039774
- **latitude**: min=None, q1=None, median=None, q3=None, p95=None, max=None, mean=None, std=None
- **longitude**: min=None, q1=None, median=None, q3=None, p95=None, max=None, mean=None, std=None
- **scheduled_quantity**: min=0.0, q1=0.0, median=1.0, q3=3000.0, p95=136034.64999999935, max=233142857.0, mean=43840.83180491494, std=1615650.605813959

## Top categories (sample)
- **pipeline_name**: Northern Natural Gas Company (20928), Columbia Gas Transmission, LLC (12151), Enable Gas Transmission (11345), Texas Eastern Transmission, LP (10455), Tennessee Gas Pipeline Company (9421), Gulf South Pipeline Company, LP (8655), Southern Star Central Gas Pipeline, Inc. (7801), Natural Gas Pipeline Company of America LLC (7288), Transcontinental Gas Pipe Line Company, LLC (5920), Northwest Pipeline GP (5890)
- **loc_name**: Diversified Production LLC (393), Transco (122), DCP OPER/NGPL KERNS #1 CADDO (72), Florida Gas Transmission (72), SLNG/EEC   CHATHAM (71), Destin (68), ETC Tiger Pipeline (68), Tennessee Wellsboro (64), BYBEE (63), Tennessee (61)
- **connecting_pipeline**: None (156875), Tennessee Gas Pipeline Company (2188), Texas Eastern Transmission, LP (1937), Transcontinental Gas Pipe Line Company, LLC (1471), ANR Pipeline Company (1348), Gulf South Pipeline Company, LP (1307), Dominion Transmission, Inc. (1051), Natural Gas Pipeline Company of America LLC (1039), Columbia Gas Transmission, LLC (1017), Enable Gas Transmission (860)
- **connecting_entity**: None (24204), 0 (4899), ENABLE GAS TRANSMISSION, (4819), BLACK HILLS UTILITY HOLDINGS, INC. (3451), MIDAMERICAN ENERGY COMPANY (2729), Montana-Dakota Utilities Co. (2207), CENTERPOINT ENERGY MINNESOTA GAS (2100), MINNESOTA ENERGY RESOURCES CORPORATION (1969), INTERSTATE POWER AND LIGHT COMPANY (1712), Kansas Gas Service, a division of ONE Gas, Inc. (1588)
- **category_short**: LDC (64312), Production (51355), Interconnect (39429), Industrial (18955), Power (9622), Storage (6432), Segment (5709), Accounting (1255), Pooling Point (1068), LNG (619)
- **country_name**: United States (200000)
- **state_abb**: LA (27291), TX (19305), PA (13970), KS (8388), OK (7868), AR (7600), IA (7181), MN (6991), MS (6655), IL (6251)
- **county_name**: None (20248), Jefferson (2356), Greene (2322), Washington (1784), Vermilion Parish (1761), Cameron Parish (1594), Weld (1562), Lincoln (1516), Jackson (1478), Sweetwater (1431)
