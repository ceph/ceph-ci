import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StorageInsightsNotificationComponent } from './storage-insights-notification.component';

describe('StorageInsightsNotificationComponent', () => {
  let component: StorageInsightsNotificationComponent;
  let fixture: ComponentFixture<StorageInsightsNotificationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [StorageInsightsNotificationComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(StorageInsightsNotificationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
