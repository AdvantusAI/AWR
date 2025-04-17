# warehouse_replenishment/batch/__init__.py

# Import functions from batch modules
from .nightly_job import run_nightly_job
from .period_end_job import run_period_end_job
from .time_based_params import process_time_based_parameters

# Try to import weekly_job, catch the error if it doesn't exist
try:
    from .weekly_job import run_weekly_job
except ImportError:
    # Create a placeholder function if the module doesn't exist
    def run_weekly_job(*args, **kwargs):
        raise NotImplementedError("weekly_job module is not implemented")

__all__ = [
    'run_nightly_job',
    'run_weekly_job',
    'run_period_end_job',
    'process_time_based_parameters'
]