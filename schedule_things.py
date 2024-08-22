import logging
from datetime import datetime
import schedule
import time
import concurrent.futures

from py_compile import compile
 
compile("fyp_main.py")
import fyp_main as fyp
compile("get_baseline_log.py")
from get_baseline_log import get_baseline_log
compile("download_videos.py")
from download_videos import download_videos
compile("transcribe_audio.py")
from transcribe_audio import transcribe_audio
compile("analyze_videos.py")
from analyze_videos import analyze_videos


def get_baseline_w_script():
    get_baseline_log("zeeschuimer_tiktok_firefox")


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


    #fyp.update_config([('input_paths', 'zeeschuimer_path'),
    #                ('result_paths', 'main_data_dir'),
    #                ('misc','label'),
    #                ('video_storage','GCP_bucket')])


    print("\n\nThis schedule will download videos, extract audio, transcribe audio, analyze videos.")
    print("Make sure your configuration files are set up correctly!")
    
    schedule.every().day.at("07:00").do(run_job, get_baseline_w_script)
    schedule.every().day.at("13:00").do(run_job, get_baseline_w_script)
    schedule.every().day.at("20:00").do(run_job, get_baseline_w_script)

    schedule.every().day.at("07:05").do(run_job, download_videos)
    schedule.every().day.at("13:05").do(run_job, download_videos)
    schedule.every().day.at("20:05").do(run_job, download_videos)

    schedule.every().day.at("07:10").do(run_job, analyze_videos)
    schedule.every().day.at("13:10").do(run_job, analyze_videos)
    schedule.every().day.at("20:10").do(run_job, analyze_videos)

    schedule.every().day.at("07:15").do(run_job, transcribe_audio)
    schedule.every().day.at("13:15").do(run_job, transcribe_audio)
    schedule.every().day.at("20:15").do(run_job, transcribe_audio)

    go_ahead = input("Do you want to start the schedule? Press Enter to continue...")


    if go_ahead != "":
        print("Exiting...")
        return


    print(f"Starting scheduler... {datetime.now()}")

    # Main loop to run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(5)



if __name__ == "__main__":
    main()
