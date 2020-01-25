import pandas as pd
import os

def get_table(team, year, outpath):
	# get data:
    url = 'https://www.pro-football-reference.com/teams/{}/{}.htm'.format(team, year)
    html = pd.read_html(url)

    # schedule and game results is the second table on the page
    game_results = html[1]
    
    if not os.path.exists(outpath): #mkdir
        os.mkdir(outpath)
   
    # save df as csv
    path = 'data/{}_{}.csv'.format(team, year)
    game_results.to_csv(path)

def get_data(teams, years, outpath):
	
	for team in teams:
	    for year in years:
	    	get_table(team, year, outpath)     
	        