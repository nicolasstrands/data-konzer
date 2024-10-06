# konzé tonton! 🥳

## Dataset

### View Datasets
- [Mauritius](https://github.com/nicolasstrands/mauritius-public-holidays-dataset/blob/main/data/public-holidays-mauritius.json)

The aim of this repository and dataset is to provide a hassle-free way to use the data to build applications.

## Disclaimers

- The information from the dataset is provided by the Prime Minister's Office of Mauritius.
- The maintainer of this repository is NOT affiliated with the Prime Minister's Office of Mauritius.
- The data is automatically fetched from the PMO's communiqué but not provided directly by them.
- The page from which the data is fetched is [publicly available](https://pmo.govmu.org/Communique/Notice%20-%20Final%20Public%20holidays%20-%202024.pdf).
- The data is made available here under fair use.

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