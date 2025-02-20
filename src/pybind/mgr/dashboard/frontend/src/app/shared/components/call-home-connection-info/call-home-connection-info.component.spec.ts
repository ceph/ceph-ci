import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CallHomeConnectionInfoComponent } from './call-home-connection-info.component';

describe('CallHomeConnectionInfoComponent', () => {
  let component: CallHomeConnectionInfoComponent;
  let fixture: ComponentFixture<CallHomeConnectionInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CallHomeConnectionInfoComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CallHomeConnectionInfoComponent);
    component = fixture.componentInstance;
    component.message = 'sample message'
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show the message', () => {
    expect(component.message).toBe('sample message');
  });
});
