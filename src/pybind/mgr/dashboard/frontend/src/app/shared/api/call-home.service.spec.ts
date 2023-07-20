import { TestBed } from '@angular/core/testing';

import { CallHomeService } from './call-home.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CallHomeService', () => {
  let service: CallHomeService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CallHomeService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CallHomeService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
