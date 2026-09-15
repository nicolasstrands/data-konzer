# konzé tonton! 🥳

## Dataset

### View datasets

- [Mauritius](data/public-holidays-mu.json)
- [France](data/public-holidays-fr.json)
- [Singapore](data/public-holidays-sg.json)
- [South Africa](data/public-holidays-za.json)
- [Turkey](data/public-holidays-tr.json)

The aim of this repository and dataset is to provide a hassle-free way to use the data to build applications.

## Disclaimers

- The information in the Mauritius dataset is provided by the Government of Mauritius.
- The maintainer of this repository is NOT affiliated with the Prime Minister's Office of Mauritius.
- The data is automatically fetched from official sources but is not provided directly by them.
- Source pages for every supported year are listed in [`links.json`](links.json).
- The data is made available here under fair use.

## Adding a new year

Add one entry to the relevant country in `links.json`:

```json
"2028": {
  "url": "https://govmu.org/EN/newsgov/SitePages/Public-Holidays-2028.aspx",
  "parser": "govmu"
}
```

Then run `python fetch.py`. The new year is fetched, validated, and merged into
the existing country file without modifying historical years. Run
`python fetch.py --check` for offline validation.

## FAQ

<details>
  <summary>In which format is the data provided?</summary>
  
- JSON
- The data structure is as follows:

```js

{
  "year": [
    {
      "name": string, // "New Year's Day",
      "date": string, // "2023-01-01",
    }
  ]
}
```

</details>

** The exact date of this festival is subject to confirmation as its celebration depends on the visibility of the moon.
