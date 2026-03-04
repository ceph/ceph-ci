import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  Inject,
  inject,
  OnInit,
  Optional,
  Output,
  ViewChild
} from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { jsPDF } from 'jspdf';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-license-agreement',
  standalone: false,
  templateUrl: './license-agreement.component.html',
  styleUrl: './license-agreement.component.scss'
})
export class LicenceAgreementComponent extends BaseModal implements OnInit, AfterViewInit {
  @Output()
  acceptanceEvent = new EventEmitter<boolean>();
  @ViewChild('licenseTextarea') textarea!: ElementRef<HTMLTextAreaElement>;

  licenceForm: CdFormGroup;

  licenceFetchingError = false;
  loading = true;
  loadingMessage = 'Please wait while we fetch the license agreement...';
  readingProgress = 10;

  private cdr = inject(ChangeDetectorRef);
  private clusterService = inject(ClusterService);

  constructor(@Optional() @Inject('customImageName') private customImageName: string) {
    super();
  }

  ngOnInit(): void {
    this.licenceForm = new CdFormGroup({
      licenceText: new FormControl(),
      accepted: new FormControl({ value: false, disabled: true }, Validators.required)
    });
    this.fetchLicenceInfo();
  }

  ngAfterViewInit(): void {
    this.cdr.detectChanges();
  }

  fetchLicenceInfo() {
    this.clusterService.getLicense(this.customImageName).subscribe({
      next: (response: string) => {
        this.licenceForm.get('licenceText')?.setValue(response);
        this.loading = false;

        this.cdr.detectChanges();

        setTimeout(() => {
          const el = this.textarea?.nativeElement;

          if (!el) return;

          if (el.scrollHeight <= el.clientHeight) {
            this.readingProgress = 100;
            this.licenceForm.get('accepted')?.enable();
            this.cdr.detectChanges();
          }
        });
      },
      error: () => {
        this.licenceFetchingError = true;
        this.loading = false;
      }
    });
  }

  accept() {
    this.acceptanceEvent.emit(true);
    this.closeModal();
  }

  reject() {
    this.acceptanceEvent.emit(false);
    this.closeModal();
  }

  download() {
    const licenceText = this.licenceForm.get('licenceText')?.value || '';
    const doc = new jsPDF();
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(10);

    const margin = 15;
    const pageWidth = doc.internal.pageSize.getWidth();
    const pageHeight = doc.internal.pageSize.getHeight();
    const maxLineWidth = pageWidth - margin * 2;
    const textLines = doc.splitTextToSize(licenceText, maxLineWidth);

    let y = margin;
    textLines.forEach((line: string) => {
      if (y > pageHeight - margin) {
        doc.addPage();
        y = margin;
      }
      doc.text(line, margin, y);
      y += 5;
    });

    doc.save(`${this.customImageName}-license-agreement.pdf`);
  }

  onScroll(event: Event) {
    if (this.readingProgress === 100) return;
    const target = event.target as HTMLElement;

    const { scrollTop, scrollHeight, clientHeight } = target;

    if (scrollHeight <= clientHeight) {
      this.readingProgress = 100;
      return;
    }

    const scrollableDist = scrollHeight - clientHeight;
    const scrolledPercent = (scrollTop / scrollableDist) * 100;

    this.readingProgress = Math.max(this.readingProgress, Math.floor(scrolledPercent));
    if (this.readingProgress === 100) this.licenceForm.get('accepted')?.enable();
  }
}
