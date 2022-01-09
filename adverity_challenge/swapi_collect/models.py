import os.path

from django.conf import settings
from django.db import models


class SwapiCollection(models.Model):
    file = models.FileField(upload_to=settings.COLLECTIONS_DIR)
    created_at = models.DateTimeField(auto_now_add=True)
    generation_time = models.FloatField()

    @property
    def filename(self):
        return os.path.basename(self.file.name)


    
