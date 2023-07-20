import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CallHomeNotificationComponent } from './call-home-notification.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '../../shared.module';

describe('CallHomeNotificationComponent', () => {
  let component: CallHomeNotificationComponent;
  let fixture: ComponentFixture<CallHomeNotificationComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, ToastrModule.forRoot(), SharedModule]
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CallHomeNotificationComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CallHomeNotificationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
