import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable } from 'rxjs';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './critical-confirmation-modal.component.html',
  styleUrls: ['./critical-confirmation-modal.component.scss']
})
export class CriticalConfirmationModalComponent implements OnInit {
  @ViewChild(SubmitButtonComponent, { static: true })
  submitButton: SubmitButtonComponent;
  bodyTemplate: TemplateRef<any>;
  bodyContext: object;
  submitActionObservable: () => Observable<any>;
  callBackAtionObservable: () => Observable<any>;
  submitAction: Function;
  backAction: Function;
  deletionForm: CdFormGroup;
  itemDescription: 'entry';
  itemNames: string[];
  actionDescription = 'delete';
  infoMessage: string;

  childFormGroup: CdFormGroup;
  childFormGroupTemplate: TemplateRef<any>;

  constructor(public activeModal: NgbActiveModal) {}

  ngOnInit() {
    const controls = {
      confirmation: new UntypedFormControl(false, [Validators.requiredTrue])
    };
    if (this.childFormGroup) {
      controls['child'] = this.childFormGroup;
    }
    this.deletionForm = new CdFormGroup(controls);
    if (!(this.submitAction || this.submitActionObservable)) {
      throw new Error('No submit action defined');
    }
  }

  callSubmitAction() {
    if (this.submitActionObservable) {
      this.submitActionObservable().subscribe({
        error: this.stopLoadingSpinner.bind(this),
        complete: this.hideModal.bind(this)
      });
    } else {
      this.submitAction();
    }
  }

  callBackAction() {
    if (this.callBackAtionObservable) {
      this.callBackAtionObservable().subscribe({
        error: this.stopLoadingSpinner.bind(this),
        complete: this.hideModal.bind(this)
      });
    } else {
      this.backAction();
    }
  }

  hideModal() {
    this.activeModal.close();
  }

  stopLoadingSpinner() {
    this.deletionForm.setErrors({ cdSubmitButton: true });
  }
}
