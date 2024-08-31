import logging
from datetime import datetime
import schedule
import time
import concurrent.futures

from fyp import *

def get_baseline_w_script():
    get_baseline_log("click_scripts/z_quick.scrpt")


def run_job(job_func):
    logging.info(f"Starting: {job_func.__name__}")
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(job_func)
            future.result()  # Wait for the job to complete
        logging.info(f"Completed: {job_func.__name__}")
    except Exception as e:
        logging.error(f"Error running job {job_func.__name__}: {e}")



def main():
    
    # Configure logging
    logging.basicConfig(
        filename='scheduler.log',  # Log file name
        level=logging.INFO,        # Log level
        format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
    )



    #print("\n\nThis schedule will download videos, extract audio, transcribe audio, analyze videos.")
    #print("Make sure your configuration files are set up correctly!")
    
    schedule.every().day.at("16:51").do(run_job, get_baseline_w_script)
    #schedule.every().day.at("13:00").do(run_job, get_baseline_w_script)
    #schedule.every().day.at("20:00").do(run_job, get_baseline_w_script)

    schedule.every().day.at("16:54").do(run_job, download_videos)
    #schedule.every().day.at("13:05").do(run_job, download_videos)
    #schedule.every().day.at("20:05").do(run_job, download_videos)

    schedule.every().day.at("16:58").do(run_job, analyze_videos)
    #schedule.every().day.at("13:10").do(run_job, analyze_videos)
    #schedule.every().day.at("20:10").do(run_job, analyze_videos)

    schedule.every().day.at("16:59").do(run_job, transcribe_audio)
    #schedule.every().day.at("13:15").do(run_job, transcribe_audio)
    #schedule.every().day.at("20:15").do(run_job, transcribe_audio)

    #go_ahead = input("Do you want to start the schedule? Press Enter to continue...")


    #if go_ahead != "":
    #    print("Exiting...")
    #    return


    print(f"Starting scheduler... {datetime.now()}")

    # Main loop to run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(5)



if __name__ == "__main__":
    main()
