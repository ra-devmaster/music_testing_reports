from dotenv import load_dotenv
from ra_instance_spinner import initial_spin, run

load_dotenv()

if __name__ == "__main__":
    initial_spin()
    run()