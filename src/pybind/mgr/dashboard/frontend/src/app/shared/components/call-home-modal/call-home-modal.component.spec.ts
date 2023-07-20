import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CallHomeModalComponent } from './call-home-modal.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { SharedModule } from '../../shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';

describe('CallHomeModalComponent', () => {
  let component: CallHomeModalComponent;
  let fixture: ComponentFixture<CallHomeModalComponent>;

  configureTestBed({
    declarations: [CallHomeModalComponent],
    imports: [SharedModule, HttpClientTestingModule, ToastrModule.forRoot(), RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CallHomeModalComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CallHomeModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
