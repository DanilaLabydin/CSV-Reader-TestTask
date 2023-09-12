from memory_profiler import profile

from main import insert_game_data


@profile
def memory_usage_test():
    insert_game_data()
 

memory_usage_test()
