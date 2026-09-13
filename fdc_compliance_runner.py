import asyncio
from fdc_compliance_spec import main as main_spec
from fdc_compliance_fetch import main as main_fetch
from fdc_compliance_report import main as main_report

async def main():
    print('#### Collecting Specs ####')
    await main_spec()
    print('#### Fetching new week data and purging old week data ####')
    await main_fetch()
    print('#### Generating Report ####')
    await main_report()

asyncio.run(main())
