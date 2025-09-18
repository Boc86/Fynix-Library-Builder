
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend

def main():
    """Runs the library update process non-interactively."""
    print("Starting scheduled library update...")
    
    def progress_callback(message):
        # Simple print callback for non-interactive scripts
        print(message)

    success = backend.run_library_update(progress_callback=progress_callback)
    
    if success:
        print("Scheduled library update completed successfully.")
        return 0
    else:
        print("Scheduled library update failed. Check fynix_library_builder.log for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
