import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { LicenceAgreementComponent } from './license-agreement.component';
import { CheckboxModule } from 'carbon-components-angular';

jest.mock('jspdf', () => ({
  jsPDF: jest.fn().mockImplementation(() => ({
    text: jest.fn(),
    addImage: jest.fn(),
    save: jest.fn()
  }))
}));

describe('LicenceAgreementComponent', () => {
  let component: LicenceAgreementComponent;
  let fixture: ComponentFixture<LicenceAgreementComponent>;

  configureTestBed({
    declarations: [LicenceAgreementComponent],
    imports: [
      FormsModule,
      ToastrModule.forRoot(),
      SharedModule,
      ReactiveFormsModule,
      CheckboxModule
    ],
    providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LicenceAgreementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize licenceForm with correct controls', () => {
    expect(component.licenceForm.get('licenceText')).toBeTruthy();
    expect(component.licenceForm.get('accepted')).toBeTruthy();
    expect(component.licenceForm.get('accepted')?.disabled).toBe(true);
  });

  it('should fetch licence info on init', () => {
    const mockResponse = 'Test license agreement text';
    jest.spyOn(component['clusterService'], 'getLicense').mockReturnValue({
      subscribe: (handlers: any) => {
        handlers.next(mockResponse);
        return { unsubscribe: jest.fn() };
      }
    } as any);

    component.fetchLicenceInfo();

    expect(component.licenceForm.get('licenceText')?.value).toBe(mockResponse);
    expect(component.loading).toBe(false);
  });

  it('should set licenceFetchingError on fetch error', () => {
    jest.spyOn(component['clusterService'], 'getLicense').mockReturnValue({
      subscribe: (handlers: any) => {
        handlers.error();
        return { unsubscribe: jest.fn() };
      }
    } as any);

    component.fetchLicenceInfo();

    expect(component.licenceFetchingError).toBe(true);
    expect(component.loading).toBe(false);
  });

  it('should accept license and emit event', () => {
    jest.spyOn(component.acceptanceEvent, 'emit');
    jest.spyOn(component, 'closeModal');
    jest.spyOn(component['clusterService'], 'acceptLicense').mockReturnValue({
      subscribe: (handlers: any) => {
        handlers.next();
        return { unsubscribe: jest.fn() };
      }
    } as any);

    component.accept();

    expect(component.acceptanceEvent.emit).toHaveBeenCalledWith(true);
    expect(component.closeModal).toHaveBeenCalled();
  });

  it('should reject license and emit event', () => {
    jest.spyOn(component.acceptanceEvent, 'emit');
    jest.spyOn(component, 'closeModal');

    component.reject();

    expect(component.acceptanceEvent.emit).toHaveBeenCalledWith(false);
    expect(component.closeModal).toHaveBeenCalled();
  });

  it('should update reading progress on scroll', () => {
    const mockEvent = {
      target: {
        scrollTop: 25,
        scrollHeight: 100,
        clientHeight: 50
      }
    } as any;

    component.onScroll(mockEvent);

    expect(component.readingProgress).toBeGreaterThan(0);
  });

  it('should handle full content visible without scrolling', () => {
    const mockEvent = {
      target: {
        scrollTop: 0,
        scrollHeight: 50,
        clientHeight: 100
      }
    } as any;

    component.onScroll(mockEvent);

    expect(component.readingProgress).toBe(100);
  });
});
