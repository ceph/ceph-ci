import { TestBed } from '@angular/core/testing';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { StorageInsightsNotificationService } from './storage-insights-notification.service';

describe('StorageInsightsNotificationService', () => {
  let service: StorageInsightsNotificationService;

  configureTestBed({
    providers: [StorageInsightsNotificationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(StorageInsightsNotificationService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
