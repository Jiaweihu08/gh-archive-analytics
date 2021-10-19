# Analyzing [GH Archive](https://www.gharchive.org)

## 1. Clone repo
```bash
git clone git@github.com:Jiaweihu08/qbeast-use-cases.git
```

## 2. Install dependencies
```bash
cd use-cases/github-analytics

conda env create -f environment.yml

conda activate github_analysis
```

## 3. Download dataset(Optional if reading data locally)
```bash
mkdir /tmp/gh_archive /tmp/gh_archive/json

wget -P /tmp/gh_archive/json https://data.gharchive.org/2021-10-03-{0..23}.json.gz
```

## 4. Prepare data(Optional if reading data locally)
```bash
python process_data.py
```

## 5. Run Streamlit app
```bash
streamlit run app.py
```




