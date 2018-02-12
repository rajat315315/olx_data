import requests
from bs4 import BeautifulSoup
from time import sleep
from multiprocessing import Pool

def get_listing(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    html = None
    links = None

    r = requests.get(url, headers=headers, timeout=10)

    if r.status_code == 200:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        # listing_section = soup.select('#offers_table table > tbody > tr > td > table > tbody > tr > td > div > span > a')
        listing_section = soup.findAll("a", {"class": "marginright5"})
        links = [link['href'].strip() for link in listing_section]
        print(links)
    return links[3:]


# parse a single item to get information
def parse(url):
    try:
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
        r = requests.get(url, headers=headers, timeout=10)
        sleep(2)

        info = []
        title_text = '-'
        location_text = '-'
        price_text = '-'
        title_text = '-'
        images = '-'
        description_text = '-'

        if r.status_code == 200:
            print('Processing..' + url)
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            title = soup.find('h1')
            if title is not None:
                title_text = title.text.strip()

            location = soup.find('strong', {'class': 'c2b small'})
            if location is not None:
                location_text = location.text.strip()

            time = soup.select('.brlefte5')
            if time is not None:
                # print(time)
                time_text = time[0].get_text().replace('\n\n', '\n')

            price = soup.find('strong', {'class': 'xxxx-large'})
            if price is not None:
                price_text = price.text.strip()

            userdetails = soup.select('.userdetails > span')
            if userdetails is not None:
                userdetails_text = userdetails[0].text.strip()

            images = soup.select('#bigGallery > li > a')
            img = [image['href'].strip() for image in images]
            images = '^'.join(img)

            description = soup.select('#textContent > p')
            if description is not None:
                description_text = description[0].text.strip()

            views = soup.select('#offerbottombar > div > strong')
            if views is not None:
                views_text = views[0].text.strip()

            info.append(url)
            info.append(title_text)
            info.append(location_text)
            info.append(time_text)
            info.append(price_text)
            info.append(userdetails_text)
            info.append(images)
            info.append(views_text)

            print(info)
    except:
        print("Exception raised")
        return ''
    return '###'.join(info)

if __name__ == '__main__':
    cars_links = []
    cars_info = []
    pages_list = []

    for x in range(1, 500):
        pages_list.append('https://www.olx.in/electronics-appliances/?page=' + str(x))

    with Pool(50) as p:
        cars_links = p.map(get_listing, pages_list)

    print(cars_links)

    flat_list = [item for sublist in cars_links for item in sublist]

    with Pool(50) as p:
        records = p.map(parse, flat_list)

    if len(records) > 0:
        with open('data_parallel_electronics.csv', 'a+', encoding='utf-8') as f:
            f.write('\n'.join(records))