import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CallHomeConnectivityStatusComponent } from './call-home-connectivity-status.component';
import { By } from '@angular/platform-browser';
import { SharedModule } from '../../shared.module';

describe('CallHomeConnectivityStatusComponent', () => {
  let component: CallHomeConnectivityStatusComponent;
  let fixture: ComponentFixture<CallHomeConnectivityStatusComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CallHomeConnectivityStatusComponent],
      imports: [SharedModule]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CallHomeConnectivityStatusComponent);
    component = fixture.componentInstance;
    component.status = {
      connectivity: true,
      last_checked: 'few seconds ago',
      connectivity_error: ''
    };
    fixture.detectChanges();
    fixture.detectChanges();
  });

  const getElementById = (id: string) => {
    return fixture.debugElement.query(By.css(`#${id}`));
  }

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display Active status when connectivity is true and no error', () => {
    const element = getElementById('active-no-error');
    expect(element.nativeElement.textContent.trim()).toBe('Active');
  });

  it('should display Inactive status when there is an error in connectivity', () => {
    component.status = {
      connectivity: false,
      last_checked: 'a while ago',
      connectivity_error: 'an error in connectivity'
    };
    fixture.detectChanges();

    const element = getElementById('inactive-with-error');
    expect(element.nativeElement.textContent.trim()).toBe('Inactive');
  });

  it('should display Active status even if there are errors', () => {
    component.status = {
      connectivity: true,
      last_checked: 'a while ago',
      connectivity_error: 'an error in connectivity'
    };
    fixture.detectChanges();

    const element = getElementById('active-no-error');
    expect(element.nativeElement.textContent.trim()).toBe('Active');
  });
});
