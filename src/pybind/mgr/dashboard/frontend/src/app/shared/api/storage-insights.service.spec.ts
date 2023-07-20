import { TestBed } from '@angular/core/testing';

import { StorageInsightsService } from './storage-insights.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('StorageInsightsService', () => {
  let service: StorageInsightsService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [StorageInsightsService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(StorageInsightsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
