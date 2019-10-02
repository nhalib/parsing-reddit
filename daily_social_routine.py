from reddit_heat import daily_reddit_heat_routine
from q_reddit_baseline import main_reddit_bl
stat = False
daily_reddit_heat_routine()
if stat:
    main_reddit_bl()