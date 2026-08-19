from prefect import serve, flow
from Scrape_Automation.prefect_scrape import main as scrape_main
from Scrape_Automation.prefect_players import main as players_main
from Scrape_Automation.prefect_drivedetails import main as drivedetails_main

@flow(name="sequential-scraping-pipeline")
def scraping_pipeline():
    """Run all scraping flows in sequence"""
    scrape_main()  # Runs first
    players_main()  # Runs after scrape completes
    drivedetails_main()  # Runs after players completes

if __name__ == "__main__":
    deployment = scraping_pipeline.to_deployment(
        name="thursday-scraping",
        cron="0 7 * * 4",  # Thursdays at 7 AM UTC (1 AM CST)
        tags=["scraping", "pipeline", "sequential"]
    )
    
    serve(deployment)

