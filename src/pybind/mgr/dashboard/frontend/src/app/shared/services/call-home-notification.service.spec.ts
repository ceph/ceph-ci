import { TestBed } from '@angular/core/testing';

import { CallHomeNotificationService } from './call-home-notification.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('CallHomeNotificationService', () => {
  let service: CallHomeNotificationService;

  configureTestBed({
    providers: [CallHomeNotificationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CallHomeNotificationService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
