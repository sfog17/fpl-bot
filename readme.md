# Instructions

1. Create conda environment
```
conda env create -f environment.yml
```

2. Install Package
```
python setup.py install
```

# Roadmap

## High-Level
- Predict best transfers based on existing team (from fpl website)
- Provide Predictions for current gameweek
- Run Season (to compare algorithms)


## To Do
### Basic Functionalities
- Add Features
- Predict Scores
- Optimise Team Selection (basic - 11 players)
- 

### Advanced Functionalities 
- Optimise with subs

### Automate
- Automate Pipeline (Luigi/Airflow)


# Backlog issues

- Install doesn't work (require `pip install --editable .`) to recognise module imports
- Logging when calling directly `scrape.py` doesn't work