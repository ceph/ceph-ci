import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StorageInsightsModalComponent } from './storage-insights-modal.component';

describe('StorageInsightsModalComponent', () => {
  let component: StorageInsightsModalComponent;
  let fixture: ComponentFixture<StorageInsightsModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [StorageInsightsModalComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(StorageInsightsModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
