import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import json
import aiofiles
import re


url = 'https://www.worldometers.info/geography/alphabetical-list-of-countries/'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml',
}


async def get_gdp_data(session, link, country):
    async with session.get(url=link, headers=headers) as response:
        print(link)
        country_gdp_list = []
        soup = BeautifulSoup(await response.text(), 'lxml')
        data = soup.find('div', class_="table-responsive")
        data1 = data.find('tbody')
        data2 = data1.find_all('tr')
        for i in data2:
            data3 = i.find_all('td')
            country_gdp_data = {
                'Country': country,
                'Year': int(data3[0].text.strip()),
                'GDP Nominal': int(data3[1].text.replace(',', '').replace('$', '').strip()),
                'GDP Real': int(data3[2].text.replace(',', '').replace('$', '').strip()),
                'GDP Per Capita': int(data3[4].text.replace(',', '').replace('$', '').strip()),
            }
            country_gdp_list.append(country_gdp_data)

        return country_gdp_list


async def get_population_data(session, link, country):
    async with session.get(link) as response:
        soup = BeautifulSoup(await response.text(), 'lxml')
        data = soup.find('div', class_="table-responsive")
        data = data.find('tbody')
        data1 = data.find_all('tr')
        country_general_data = {}
        country_yearly_data = []

        for i in data1:
            data2 = i.find_all('td')
            try:
                try:
                    up = int(data2[9].text.replace(',', '').strip())
                except Exception as e:
                    up = None
                country_yearly_data.append({
                    'Country': country,
                    'Year': int(data2[0].text.strip()),
                    'Population': int(data2[1].text.replace(',', '').strip()),
                    'Median Age': round(float(data2[5].text.strip()), 2),
                    'Fertility Rate': round(float(data2[6].text.strip()), 2),
                    'Density': int(data2[7].text.replace(',', '').strip()),
                    'Urban Population': up,
                    'World Population': int(data2[11].text.replace(',', '').strip()),
                })
            except Exception as e:
                try:
                    up = int(data2[6].text.replace(',', '').strip())
                except Exception as e:
                    up = None
                country_yearly_data.append({
                    'Country': country,
                    'Year': int(data2[0].text.strip()),
                    'Population': int(data2[1].text.replace(',', '').strip()),
                    'Median Age': None,
                    'Fertility Rate': None,
                    'Density': int(data2[4].text.replace(',', '').strip()),
                    'Urban Population': up,
                    'World Population': int(data2[8].text.replace(',', '').strip()),
                })

        try:
            data3 = soup.find('div', class_="row", style="margin-top:50px")
            data4 = data3.find_all('span', style="font-size:22px; font-weight:bold;")
            country_general_data.update({
                'Life Expectancy': round(float(data4[0].text.replace('years', '').strip()), 2),
                'Infant Mortality': round(float(data4[1].text.replace('years', '').strip()), 2),
                'Death Under Age 5': round(float(data4[2].text.replace('years', '').strip()), 2),
            })
        except Exception as e:
            country_general_data.update({
                'Life Expectancy': None,
                'Infant Mortality': None,
                'Death Under Age 5': None,
            })

        try:
            city_data = soup.find('div', class_="table-responsive", style="clear:both; width:100%; max-width:500px ")
            city_data = city_data.find('tbody')
            cities = city_data.find_all('tr')

            country_general_data.update({
                'Most Populated City Name': cities[0].find_all('td')[1].text,
                'Most Populated City Population': int(cities[0].find_all('td')[2].text.replace(',', '').strip()),
                'Least Populated City Name': cities[-1].find_all('td')[1].text,
                'Least Populated City Population': int(cities[-1].find_all('td')[2].text.replace(',', '').strip())
            })
        except Exception as e:
            country_general_data.update({
                'Most Populated City Name': None,
                'Most Populated City Population': None,
                'Least Populated City Name': None,
                'Least Populated City Population': None,
            })

        try:
            gdp_link = soup.find('div', class_='spaced').find_all('li')[4].find('a')['href']
            gdp_link = 'https://www.worldometers.info' + gdp_link
            gdp_data = await get_gdp_data(session=session, link=gdp_link, country=country)
        except Exception as e:
            gdp_data = {
                'Country': country,
                'Year': None,
                'GDP Nominal': None,
                'GDP Real': None,
                'GDP Per Capita': None,
            }

        return [country_general_data, country_yearly_data, gdp_data]


async def get_main_data():
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, headers=headers) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
            data = soup.find('tbody')
            data1 = data.find_all('tr')
            links = []
            country_general_list = []
            country_population_list = []
            country_gdp_list = []
            tasks = []

            for i in data1:
                data2 = i.find_all('td')

                country_general_data = {
                    'Country': re.sub(r'\([^)]*\)', '', data2[1].text).strip(),
                    'Land Area': int(data2[3].text.replace(',', '').strip()),
                    'Density': int(data2[4].text.replace(',', '').strip()),

                }

                link = 'https://www.worldometers.info' + i.find('a')['href']
                links.append(link)

                population_data = await get_population_data(session=session, link=link,
                                                            country=country_general_data['Country'])

                country_general_data.update(population_data[0])
                country_general_list.append(country_general_data)

                country_population_list.extend(population_data[1])

                country_gdp_list.extend(population_data[2])

            async with aiofiles.open(r'data\json\country_population.json', 'w') as f:
                await f.write(json.dumps(country_population_list, indent=4))

            async with aiofiles.open(r'data\json\country_general.json', 'w') as f:
                await f.write(json.dumps(country_general_list, indent=4))

            async with aiofiles.open(r'data\json\country_gdp.json', 'w') as f:
                await f.write(json.dumps(country_gdp_list, indent=4))


def to_excel():
    with open(r'data\json\country_gdp.json', 'r') as file:
        data = json.load(file)
        country_gdp = []
        for i in data:
            if isinstance(i, dict):
                country_gdp.append(i)

    with open(r'data\json\country_general.json', 'r') as file:
        country_general = json.load(file)

    with open(r'data\json\country_population.json', 'r') as file:
        country_population = json.load(file)

    country_gdp_data = pd.DataFrame({
        "Country": [i['Country'] for i in country_gdp],
        "Year": [i['Year'] for i in country_gdp],
        "GDP Nominal": [i['GDP Nominal'] for i in country_gdp],
        "GDP Real": [i['GDP Real'] for i in country_gdp],
        "GDP Per Capita": [i['GDP Per Capita'] for i in country_gdp]
    })

    country_general_data = pd.DataFrame({
        "Country": [i['Country'] for i in country_general],
        "Land Area": [i['Land Area'] for i in country_general],
        "Density": [i['Density'] for i in country_general],
        "Life Expectancy": [i['Life Expectancy'] for i in country_general],
        "Infant Mortality": [i['Infant Mortality'] for i in country_general],
        "Death Under Age 5": [i['Death Under Age 5'] for i in country_general],
        "Most Populated City Name": [i['Most Populated City Name'] for i in country_general],
        "Most Populated City Population": [i['Most Populated City Population'] for i in country_general],
        "Least Populated City Name": [i['Least Populated City Name'] for i in country_general],
        "Least Populated City Population": [i['Least Populated City Population'] for i in country_general]
    })

    country_population_data = pd.DataFrame({
        "Country": [i['Country'] for i in country_population],
        "Year": [i['Year'] for i in country_population],
        "Population": [i['Population'] for i in country_population],
        "Median Age": [i['Median Age'] for i in country_population],
        "Fertility Rate": [i['Fertility Rate'] for i in country_population],
        "Density": [i['Density'] for i in country_population],
        "Urban Population": [i['Urban Population'] for i in country_population],
        "World Population": [i['World Population'] for i in country_population]
    })

    country_gdp_data.to_excel(r'data\excel\country_gdp_data.xlsx', index=False, sheet_name='Sheet1')
    country_general_data.to_excel(r'data\excel\country_general_data.xlsx', index=False, sheet_name='Sheet1')
    country_population_data.to_excel(r'data\excel\country_population_data.xlsx', index=False, sheet_name='Sheet1')


def main():
    asyncio.run(get_main_data())
    to_excel()


if __name__ == '__main__':
    main()


