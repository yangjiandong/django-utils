import datetime

from django.conf import settings

from djutils import dashboard
from djutils.dashboard.models import Panel, PanelData, PanelDataSet
from djutils.queue.decorators import periodic_command, crontab

dashboard.autodiscover()

# set to 0 or None to prevent data from expiring
EXPIRATION_DAYS = getattr(settings, 'PANEL_DATA_EXPIRATION_DAYS', 7)

@periodic_command(crontab())
def update_panels():
    """
    Simple task which updates the dashboard panels every minute
    """
    Panel.objects.update_panels()

@periodic_command(crontab(minute=0, hour='*'))
def generate_hourly_aggregates():
    Panel.objects.generate_hourly_aggregates()

@periodic_command(crontab(minute=0, hour=0))
def generate_daily_aggregates():
    Panel.objects.generate_daily_aggregates()

@periodic_command(crontab(minute=0, hour=0))
def remove_old_panel_data():
    """
    Remove old panel data
    """
    if EXPIRATION_DAYS:
        cutoff = datetime.datetime.now() - datetime.timedelta(days=EXPIRATION_DAYS)
        
        # try deleting these first so as not to load up the subsequent
        # delete while collecting sub-objects
        PanelDataSet.objects.filter(
            panel_data__created_date__lte=cutoff,
            panel_data__aggregate_type=PANEL_AGGREGATE_MINUTE,
        ).delete()

        PanelData.objects.filter(
            created_date__lte=cutoff,
            aggregate_type=PANEL_AGGREGATE_MINUTE,
        ).delete()
